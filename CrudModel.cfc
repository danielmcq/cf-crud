component {
	public any function $create () {
		var oQuery = new Query(datasource = VARIABLES.dsn);
		var sql = "";
		var colList = [];
		var paramList = [];
		var output = {created=false};

		StructEach(ARGUMENTS,function(fieldName, val){
			if (
					StructKeyExists(VARIABLES.DB_SPEC.fields, fieldName)
					&& (!StructKeyExists(VARIABLES.DB_SPEC.fields[fieldName], "readOnly") || !VARIABLES.DB_SPEC.fields[fieldName].readOnly)
					&& (!StructKeyExists(VARIABLES.DB_SPEC.fields[fieldName], "externalTable") || !VARIABLES.DB_SPEC.fields[fieldName].externalTable)
			) {
				ArrayAppend(colList, VARIABLES.DB_SPEC.fields[fieldName].column);
				ArrayAppend(paramList, fieldName);
				if (VARIABLES.DB_SPEC.fields[fieldName].type == "cf_sql_timestamp") {
					val = CreateOdbcDateTime(val);
				}
				oQuery.addParam(name=fieldName, value=val, cfsqltype=VARIABLES.DB_SPEC.fields[fieldName].type);
			}
		});

		sql &= "INSERT INTO #getFullTableName()# (#arrayToListWrap(colList, '[', ']')#) ";
		sql &= "VALUES (#arrayToListWrap(paramList, ' :')#)";

		var result = oQuery.execute(sql=sql);

		var readData = $read(ArgumentCollection={"#VARIABLES.DB_SPEC.primaryFieldName#"=result.getPrefix().generatedKey});
		if (ArrayLen(readData)) {
			output = readData[1];
		}

		return output;
	}


	public any function $read () {
		var oQuery = new Query(datasource = VARIABLES.dsn);
		var sql = "";
		var whereClause = getWhereClause(ARGUMENTS, oQuery);

		sql &= "SELECT ";
		if (StructKeyExists(ARGUMENTS, "limit") && IsNumeric(ARGUMENTS.limit)) {
			sql &= "TOP #ARGUMENTS.limit# ";
		} else if (Len(whereClause) == 0) {
			sql &= "TOP #VARIABLES.limit# ";
		}
		sql &= "#ArrayToList(getSelectColumns())# ";
		sql &= "FROM #getFullTableName()# AS #getTableAlias()# ";
		sql &= getJoinTablesClause();
		sql &= whereClause;
		sql &= getOrderByClause();

		var result = oQuery.execute(sql=sql);
		var parseParams = {
			rs = result.getResult()
		};
		if (StructKeyExists(ARGUMENTS, "afterParseFunction")) {
			parseParams.afterParseFunction = ARGUMENTS.afterParseFunction;
		}

		return recordSetToArray( ArgumentCollection=parseParams );
	}


	public any function $update () {
		var oQuery = new Query(datasource = VARIABLES.dsn);
		var sql = "";
		var output = {updated=false};
		var setList = [];

		if (StructKeyExists(ARGUMENTS, VARIABLES.DB_SPEC.primaryFieldName)) {
			sql &= "UPDATE #getFullTableName()# ";

			StructEach(ARGUMENTS, function(fieldName, val){
				if (
					StructKeyExists(VARIABLES.DB_SPEC.fields, fieldName)
					&& fieldName != VARIABLES.DB_SPEC.primaryFieldName
					&& (!StructKeyExists(VARIABLES.DB_SPEC.fields[fieldName], "externalTable") || !VARIABLES.DB_SPEC.fields[fieldName].externalTable)
					&& (!StructKeyExists(VARIABLES.DB_SPEC.fields[fieldName], "readOnly") || !VARIABLES.DB_SPEC.fields[fieldName].readOnly)
					&& (!StructKeyExists(VARIABLES.DB_SPEC.fields[fieldName], "createOnly") || !VARIABLES.DB_SPEC.fields[fieldName].createOnly)
				) {
					ArrayAppend(setList, "[#VARIABLES.DB_SPEC.fields[fieldName].column#] = :#fieldName#");
					if (VARIABLES.DB_SPEC.fields[fieldName].type == "cf_sql_timestamp") {
						val = CreateOdbcDateTime(val);
					}
					oQuery.addParam(name=fieldName, value=val, cfsqltype=VARIABLES.DB_SPEC.fields[fieldName].type);
				}
			});

			sql &= "SET #ArrayToList(setList, ', ')# ";
			sql &= "WHERE [#VARIABLES.DB_SPEC.fields[VARIABLES.DB_SPEC.primaryFieldName].column#] = :#VARIABLES.DB_SPEC.primaryFieldName# ";
			oQuery.addParam(
				name=VARIABLES.DB_SPEC.primaryFieldName
				,value=ARGUMENTS[VARIABLES.DB_SPEC.primaryFieldName]
				,cfsqltype=VARIABLES.DB_SPEC.fields[VARIABLES.DB_SPEC.primaryFieldName].type
			);

			var result = oQuery.execute(sql=sql);

			var readData = $read(ArgumentCollection={"#VARIABLES.DB_SPEC.primaryFieldName#"=ARGUMENTS[VARIABLES.DB_SPEC.primaryFieldName]});
			if (ArrayLen(readData)) {
				output = readData[1];
			}
		}

		return output;
	}


	public any function $delete () {
		var output = {deleted=false};

		output = $update(ArgumentCollection=ARGUMENTS);
		output.deleted = true;

		return output;
	}


	package string function getColumnName (required string fieldName) {
		var column = "";

		if (StructKeyExists(VARIABLES.DB_SPEC.fields, ARGUMENTS.fieldName)) {
			column = VARIABLES.DB_SPEC.fields[ARGUMENTS.fieldName].column;
		}

		return column;
	}


	package string function getFieldCfSqlType (required string fieldName) {
		var output = "";

		if (StructKeyExists(VARIABLES.DB_SPEC.fields, fieldName)) {
			output = VARIABLES.DB_SPEC.fields[fieldName].type;
		}

		return output;
	}


	package string function getFieldName (required string columnName) {
		var fieldName = "";
		var dbMap = getDbMap();

		if (StructKeyExists(dbMap, ARGUMENTS.columnName)) {
			fieldName = dbMap[ARGUMENTS.columnName];
		}

		return fieldName;
	}


	package string function getFullTableName () {
		var output = "";

		if (StructKeyExists(VARIABLES.DB_SPEC, "database") && Len(VARIABLES.DB_SPEC.database)) {
			output &= "[#VARIABLES.DB_SPEC.database#].";
		}
		if (StructKeyExists(VARIABLES.DB_SPEC, "schema") && Len(VARIABLES.DB_SPEC.schema)) {
			output &= "[#VARIABLES.DB_SPEC.schema#].";
		}
		output &= "[#VARIABLES.DB_SPEC.table#]";

		return output;
	}


	package string function getTableAlias () {
		return ListLast(GetMetaData(THIS).name, '.');
	}


	private string function arrayToListWrap (required array input, string prepend="", string append="", string delimiter=",") {
		var out = "";
		var pre = ARGUMENTS.prepend;
		var app = ARGUMENTS.append;
		var del = ARGUMENTS.delimiter;

		ArrayEach(ARGUMENTS.input, function(item, idx){
			if (idx > 1) {
				out &= del;
			}
			out &= pre&item&app;
		});

		return out;
	}


	private struct function getDbMap () {
		if (!StructKeyExists(VARIABLES, "dbMap")) {
			VARIABLES["dbMap"] = {};

			if (StructKeyExists(VARIABLES.DB_SPEC, "joinTables")) {
				StructEach(VARIABLES.DB_SPEC.joinTables, function(joinModelPath, joinTableSpec){
					var joinModel = getJoinModel(joinModelPath);

					ArrayEach(joinTableSpec.columns, function(fieldName){
						VARIABLES.dbMap[joinModel.getColumnName(fieldName)] = fieldName;
					});
				});
			}

			StructEach(VARIABLES.DB_SPEC.fields, function(fieldName, fieldSpec){
				VARIABLES.dbMap[fieldSpec.column] = fieldName;
			});
		}

		return VARIABLES.dbMap;
	}


	private api.bases.CrudModel function getJoinModel (required string joinModelPath) {
		if (!StructKeyExists(VARIABLES, "joinModels")) {
			VARIABLES["joinModels"] = {};
		}

		if (!StructKeyExists(VARIABLES.joinModels, ARGUMENTS.joinModelPath)) {
			VARIABLES.joinModels[ARGUMENTS.joinModelPath] = new "#ARGUMENTS.joinModelPath#"(appConfig=VARIABLES.appConfig);
		}

		return VARIABLES.joinModels[ARGUMENTS.joinModelPath];
	}


	private string function getJoinTablesClause () {
		var output = "";

		if (StructKeyExists(VARIABLES.DB_SPEC, "joinTables")) {
			StructEach(VARIABLES.DB_SPEC.joinTables,function(tableModelPath, tableSpec){
				var joinTableModel = getJoinModel(tableModelPath);
				var tableAlias = joinTableModel.getTableAlias();
				output &= "LEFT JOIN #joinTableModel.getFullTableName()# AS #tableAlias# ";
				var leftJoinTableAlias = getTableAlias();
				var leftJoinColumn = "";
				if (StructKeyExists(tableSpec, "leftJoinTable")) {
					var leftJoinModel = getJoinModel(tableSpec.leftJoinTable);
					leftJoinTableAlias = leftJoinModel.getTableAlias();
					leftJoinColumn = leftJoinModel.getColumnName(tableSpec.joinColumn);
				} else {
					leftJoinColumn = VARIABLES.DB_SPEC.fields[tableSpec.joinColumn].column;
				}
				output &= "ON #leftJoinTableAlias#.[#leftJoinColumn#] ";
				output &= "= #tableAlias#.[#joinTableModel.getColumnName(tableSpec.joinColumn)#] ";
			});
		}

		return output;
	}


	private string function getOrderByClause () {
		var sql = "";
		var orderByList = [];

		if (StructKeyExists(VARIABLES.DB_SPEC,"orderBy")) {
			ArrayEach(VARIABLES.DB_SPEC.orderBy, function(fieldName){
				if (Len(getColumnName(fieldName)) > 0) {
					ArrayAppend(orderByList, "#getTableAlias()#.[#getColumnName(fieldName)#]");
				} else if (StructKeyExists(VARIABLES.DB_SPEC, "joinTables")) {
					StructEach(VARIABLES.DB_SPEC.joinTables,function(joinModelPath, joinTableSpec){
						var joinTableModel = getJoinModel(joinModelPath);
						var joinFieldName = ListLast(fieldName, ".");
						var joinFieldTablePrefix = ListDeleteAt(fieldName, ListLen(fieldName, "."), ".");
						if (ArrayFind([joinModelPath,joinTableModel.getTableAlias()], joinFieldTablePrefix) && Len(joinTableModel.getColumnName(joinFieldName)) > 0) {
							ArrayAppend(orderByList, "#joinTableModel.getTableAlias()#.[#joinTableModel.getColumnName(joinFieldName)#]");
						}
					});
				}
			});

			if (ArrayLen(orderByList) > 0) {
				sql &= "ORDER BY #ArrayToList(orderByList)# ";
			}
		}

		return sql;
	}


	private array function getSelectColumns () {
		var columns = [];

		StructEach(VARIABLES.DB_SPEC.fields,function(fieldName, fieldSpec){
			var alias = getTableAlias();

			ArrayAppend(columns, "#alias#.[#fieldSpec.column#]");
		});
		if (StructKeyExists(VARIABLES.DB_SPEC, "joinTables")) {
			StructEach(VARIABLES.DB_SPEC.joinTables, function(tableModelPath, tableSpec){
				var joinTableModel = getJoinModel(tableModelPath);
				var alias = joinTableModel.getTableAlias();

				ArrayEach(tableSpec.columns, function(fieldName){
					ArrayAppend(columns, "#alias#.[#joinTableModel.getColumnName(fieldName)#]");
				});
			});
		}

		return columns;
	}


	private string function getWhereClause (required struct params, required com.adobe.coldfusion.query oQuery) {
		var sql = "";
		var whereList = [];
		var alias = getTableAlias();

		StructEach(ARGUMENTS.params, function(fieldName, fieldValue){
			if (Len(getColumnName(fieldName)) > 0) {
				ArrayAppend(whereList, "#alias#.[#getColumnName(fieldName)#] = :#fieldName#");
				oQuery.addParam(name=fieldName, value=fieldValue, cfsqltype=getFieldCfSqlType(fieldName));
			} else if (StructKeyExists(VARIABLES.DB_SPEC, "joinTables")) {
				StructEach(VARIABLES.DB_SPEC.joinTables,function(tableModelPath, tableSpec){
					var joinTableModel = getJoinModel(tableModelPath);
					if (Len(joinTableModel.getColumnName(fieldName)) > 0) {
						var tableAlias = joinTableModel.getTableAlias();
						ArrayAppend(whereList, "#tableAlias#.[#joinTableModel.getColumnName(fieldName)#] = :#tableAlias#_#fieldName#");
						oQuery.addParam(name=tableAlias&"_"&fieldName, value=fieldValue, cfsqltype=joinTableModel.getFieldCfSqlType(fieldName));
					}
				});
			}
		});

		if (ArrayLen(whereList) > 0) {
			sql &= "WHERE #ArrayToList(whereList, ' AND ')# ";
		}

		return sql;
	}


	private array function recordSetToArray (required query rs, array filter=[]) {
		var output = [];

		try {
			for (var rsRow in ARGUMENTS.rs) {
				var mappedRow = Duplicate(rsRow);

				StructEach(rsRow, function(columnName, rsCellValue){
					if (Len(getFieldName(columnName)) > 0) {
						if ( !ArrayFind(filter, columnName) ) {
							mappedRow[ getFieldName(columnName) ] = rsCellValue;
						}
						StructDelete(mappedRow, columnName);
					}
				});
				if (StructKeyExists(ARGUMENTS, "afterParseFunction")) {
					mappedRow = ARGUMENTS.afterParseFunction(mappedRow);
				}
				ArrayAppend(output, mappedRow);
			}
		} catch (any e) {
			ArrayAppend(output, e);
		}

		return output;
	}
}