package ch.ehi.ofgdb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OfgdbMetaData implements DatabaseMetaData {
	private OfgdbConnection conn=null;
	public OfgdbMetaData(OfgdbConnection conn1){
		conn=conn1;
	}
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		return arg0 != null && (arg0.isAssignableFrom(getClass()) || arg0.isAssignableFrom(OfgdbConnection.class));
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		if (arg0 == null) {
			throw new SQLException("iface is null");
		}
		if (arg0.isAssignableFrom(getClass())) {
			return arg0.cast(this);
		}
		if (arg0.isAssignableFrom(OfgdbConnection.class)) {
			return arg0.cast(conn);
		}
		throw new SQLException("not a wrapper for " + arg0.getName());
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		return emptyResultSet("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE");
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		return getPrimaryKeys(catalog, schema, table);
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "catalog";
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return emptyResultSet("TABLE_CAT");
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		return emptyResultSet("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION");
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException {
		return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE");
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		List<Map<String,Object>> rows=new ArrayList<Map<String,Object>>();
		List<String> columns=new ArrayList<String>();
		columns.add("TABLE_CAT");
		columns.add("TABLE_SCHEM");
		columns.add("TABLE_NAME");
		columns.add("COLUMN_NAME");
		columns.add("DATA_TYPE");
		columns.add("TYPE_NAME");
		columns.add("COLUMN_SIZE");
		columns.add("BUFFER_LENGTH");
		columns.add("DECIMAL_DIGITS");
		columns.add("NUM_PREC_RADIX");
		columns.add("NULLABLE");
		columns.add("REMARKS");
		columns.add("COLUMN_DEF");
		columns.add("SQL_DATA_TYPE");
		columns.add("SQL_DATETIME_SUB");
		columns.add("CHAR_OCTET_LENGTH");
		columns.add("ORDINAL_POSITION");
		columns.add("IS_NULLABLE");
		columns.add("SCOPE_CATALOG");
		columns.add("SCOPE_SCHEMA");
		columns.add("SCOPE_TABLE");
		columns.add("SOURCE_DATA_TYPE");
		columns.add("IS_AUTOINCREMENT");
		columns.add("IS_GENERATEDCOLUMN");
		for(String tableName:conn.getKnownTableNames()){
			if(!matchesPattern(tableName, tableNamePattern)){
				continue;
			}
			List<ColumnMetadata> fieldInfos = readTableColumnMetadata(tableName);
			for(int i=0;i<fieldInfos.size();i++){
				ColumnMetadata info = fieldInfos.get(i);
				String fieldName=info.fieldName;
				if(!matchesPattern(fieldName, columnNamePattern)){
					continue;
				}
				Map<String,Object> row=new HashMap<String,Object>();
				row.put("TABLE_CAT", catalog);
				row.put("TABLE_SCHEM", null);
				row.put("TABLE_NAME", tableName);
				row.put("COLUMN_NAME", fieldName);
				row.put("DATA_TYPE", Integer.valueOf(info.jdbcType));
				row.put("TYPE_NAME", info.typeName);
				row.put("COLUMN_SIZE", info.columnSize);
				row.put("BUFFER_LENGTH", null);
				row.put("DECIMAL_DIGITS", info.decimalDigits);
				row.put("NUM_PREC_RADIX", info.numPrecRadix);
				row.put("NULLABLE", Integer.valueOf(columnNullableUnknown));
				row.put("REMARKS", null);
				row.put("COLUMN_DEF", null);
				row.put("SQL_DATA_TYPE", null);
				row.put("SQL_DATETIME_SUB", null);
				row.put("CHAR_OCTET_LENGTH", info.charOctetLength);
				row.put("ORDINAL_POSITION", Integer.valueOf(i+1));
				row.put("IS_NULLABLE", "YES");
				row.put("SCOPE_CATALOG", null);
				row.put("SCOPE_SCHEMA", null);
				row.put("SCOPE_TABLE", null);
				row.put("SOURCE_DATA_TYPE", null);
				row.put("IS_AUTOINCREMENT", OfgdbTypeUtil.isPrimaryKeyColumn(fieldName) ? "YES" : "NO");
				row.put("IS_GENERATEDCOLUMN", "NO");
				rows.add(row);
			}
		}
		return new OfgdbResultSet(rows,columns);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		return emptyResultSet("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
				"FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "FK_NAME", "PK_NAME");
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return 1;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "ESRI FileGDB API";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		return "1.5";
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public int getDriverMajorVersion() {
		return 0;
	}

	@Override
	public int getDriverMinorVersion() {
		return 1;
	}

	@Override
	public String getDriverName() throws SQLException {
		return "openfgdb4j";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return String.valueOf(getDriverMajorVersion()) + "." + String.valueOf(getDriverMinorVersion());
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		return emptyResultSet("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
				"FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "FK_NAME", "PK_NAME");
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "$";
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException {
		return emptyResultSet("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE");
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {
		return emptyResultSet("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE");
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		return emptyResultSet("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
				"FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "FK_NAME", "PK_NAME");
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		List<String> columns = Arrays.asList(
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE",
				"ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION");
		String resolvedTable = conn.resolveTableName(table);
		int ordinal = 1;
		for (String columnName : readPrimaryKeyCandidates(resolvedTable)) {
			Map<String, Object> row = new LinkedHashMap<String, Object>();
			row.put("TABLE_CAT", catalog);
			row.put("TABLE_SCHEM", null);
			row.put("TABLE_NAME", resolvedTable);
			row.put("NON_UNIQUE", Boolean.FALSE);
			row.put("INDEX_QUALIFIER", null);
			row.put("INDEX_NAME", "PK_" + resolvedTable);
			row.put("TYPE", Short.valueOf((short) tableIndexClustered));
			row.put("ORDINAL_POSITION", Short.valueOf((short) ordinal++));
			row.put("COLUMN_NAME", columnName);
			row.put("ASC_OR_DESC", "A");
			row.put("CARDINALITY", null);
			row.put("PAGES", null);
			row.put("FILTER_CONDITION", null);
			rows.add(row);
		}
		return new OfgdbResultSet(rows, columns);
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return 4;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return 2;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		List<String> columns = Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME");
		String resolvedTable = conn.resolveTableName(table);
		int seq = 1;
		for (String columnName : readPrimaryKeyCandidates(resolvedTable)) {
			Map<String, Object> row = new LinkedHashMap<String, Object>();
			row.put("TABLE_CAT", catalog);
			row.put("TABLE_SCHEM", null);
			row.put("TABLE_NAME", resolvedTable);
			row.put("COLUMN_NAME", columnName);
			row.put("KEY_SEQ", Short.valueOf((short) seq++));
			row.put("PK_NAME", "PK_" + resolvedTable);
			rows.add(row);
		}
		return new OfgdbResultSet(rows, columns);
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		return emptyResultSet("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE");
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "procedure";
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {
		return emptyResultSet("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "REMARKS", "PROCEDURE_TYPE");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return "";
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return sqlStateSQL99;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return emptyResultSet("TABLE_SCHEM", "TABLE_CATALOG");
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException {
		return emptyResultSet("TABLE_SCHEM", "TABLE_CATALOG");
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "\\";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME");
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern,
			String typeNamePattern) throws SQLException {
		return emptyResultSet("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME");
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "PRIVILEGE");
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		Map<String, Object> row = new LinkedHashMap<String, Object>();
		row.put("TABLE_TYPE", "TABLE");
		rows.add(row);
		return new OfgdbResultSet(rows, Arrays.asList("TABLE_TYPE"));
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		List<Map<String,Object>> rows=new ArrayList<Map<String,Object>>();
		if (!acceptsTableType(types)) {
			return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
					"TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
		}
		for(String tableName:conn.getKnownTableNames()){
			if(!matchesPattern(tableName, tableNamePattern)){
				continue;
			}
			Map<String,Object> row=new HashMap<String,Object>();
			row.put("TABLE_CAT", catalog);
			row.put("TABLE_SCHEM", null);
			row.put("TABLE_NAME", tableName);
			row.put("TABLE_TYPE", "TABLE");
			row.put("REMARKS", null);
			row.put("TYPE_CAT", null);
			row.put("TYPE_SCHEM", null);
			row.put("TYPE_NAME", null);
			row.put("SELF_REFERENCING_COL_NAME", null);
			row.put("REF_GENERATION", null);
			rows.add(row);
		}
		List<String> columns=new ArrayList<String>();
		columns.add("TABLE_CAT");
		columns.add("TABLE_SCHEM");
		columns.add("TABLE_NAME");
		columns.add("TABLE_TYPE");
		columns.add("REMARKS");
		columns.add("TYPE_CAT");
		columns.add("TYPE_SCHEM");
		columns.add("TYPE_NAME");
		columns.add("SELF_REFERENCING_COL_NAME");
		columns.add("REF_GENERATION");
		return new OfgdbResultSet(rows,columns);
	}

	private ResultSet emptyResultSet(String... columns) {
		return new OfgdbResultSet(new ArrayList<Map<String, Object>>(), Arrays.asList(columns));
	}

	private List<String> readPrimaryKeyCandidates(String tableName) throws SQLException {
		if (tableName == null || tableName.trim().isEmpty()) {
			return new ArrayList<String>();
		}
		String resolvedTableName = conn.resolveTableName(tableName);
		OfgdbTableSchema tableSchema = conn.getTableSchema(resolvedTableName);
		List<String> keys = tableSchema.getPrimaryKeyColumns();
		if (!keys.isEmpty()) {
			return keys;
		}
		ArrayList<String> fallback = new ArrayList<String>();
		for (OfgdbColumnSchema column : tableSchema.columns) {
			if (OfgdbTypeUtil.isPrimaryKeyColumn(column.name)) {
				fallback.add(column.name);
			}
		}
		return fallback;
	}

	private List<ColumnMetadata> readTableColumnMetadata(String tableName) throws SQLException {
		List<ColumnMetadata> fields = new ArrayList<ColumnMetadata>();
		if (tableName == null || tableName.trim().isEmpty()) {
			return fields;
		}
		String resolvedTableName = conn.resolveTableName(tableName);
		OfgdbTableSchema tableSchema = conn.getTableSchema(resolvedTableName);
		for (OfgdbColumnSchema schemaColumn : tableSchema.columns) {
			ColumnMetadata meta = new ColumnMetadata(schemaColumn.name, schemaColumn.jdbcType);
			meta.typeName = schemaColumn.jdbcTypeName != null ? schemaColumn.jdbcTypeName : meta.typeName;
			meta.columnSize = schemaColumn.columnSize != null ? schemaColumn.columnSize : meta.columnSize;
			meta.decimalDigits = schemaColumn.decimalDigits != null ? schemaColumn.decimalDigits : meta.decimalDigits;
			meta.numPrecRadix = schemaColumn.numPrecRadix != null ? schemaColumn.numPrecRadix : meta.numPrecRadix;
			meta.charOctetLength = schemaColumn.charOctetLength != null ? schemaColumn.charOctetLength : meta.charOctetLength;
			meta.inferredBySampling = false;
			fields.add(meta);
		}
		return fields;
	}

	private Integer tryInferJdbcTypeFromRow(long rowHandle, String fieldName) throws ch.ehi.openfgdb4j.OpenFgdbException {
		if (conn.getApi().rowIsNull(rowHandle, fieldName)) {
			return null;
		}
		Integer intValue = tryRowGetInt32(rowHandle, fieldName);
		if (intValue != null) {
			return Integer.valueOf(Types.INTEGER);
		}
		Double doubleValue = tryRowGetDouble(rowHandle, fieldName);
		if (doubleValue != null) {
			return Integer.valueOf(Types.DOUBLE);
		}
		byte[] blobValue = tryRowGetBlob(rowHandle, fieldName);
		if (blobValue != null) {
			return Integer.valueOf(Types.VARBINARY);
		}
		String textValue = tryRowGetString(rowHandle, fieldName);
		if (textValue != null) {
			if (OfgdbTypeUtil.isLikelyGeometryColumn(fieldName) || OfgdbTypeUtil.looksBinaryText(textValue)) {
				byte[] geometry = tryRowGetGeometry(rowHandle);
				if (geometry != null) {
					return Integer.valueOf(Types.VARBINARY);
				}
			}
			return Integer.valueOf(Types.VARCHAR);
		}
		if (OfgdbTypeUtil.isLikelyGeometryColumn(fieldName)) {
			byte[] geometry = tryRowGetGeometry(rowHandle);
			if (geometry != null) {
				return Integer.valueOf(Types.VARBINARY);
			}
		}
		return null;
	}

	private Integer tryRowGetInt32(long rowHandle, String fieldName) throws ch.ehi.openfgdb4j.OpenFgdbException {
		try {
			return conn.getApi().rowGetInt32(rowHandle, fieldName);
		} catch (ch.ehi.openfgdb4j.OpenFgdbException e) {
			if (isTypeMismatch(e)) {
				return null;
			}
			throw e;
		}
	}

	private Double tryRowGetDouble(long rowHandle, String fieldName) throws ch.ehi.openfgdb4j.OpenFgdbException {
		try {
			return conn.getApi().rowGetDouble(rowHandle, fieldName);
		} catch (ch.ehi.openfgdb4j.OpenFgdbException e) {
			if (isTypeMismatch(e)) {
				return null;
			}
			throw e;
		}
	}

	private byte[] tryRowGetBlob(long rowHandle, String fieldName) throws ch.ehi.openfgdb4j.OpenFgdbException {
		try {
			return conn.getApi().rowGetBlob(rowHandle, fieldName);
		} catch (ch.ehi.openfgdb4j.OpenFgdbException e) {
			if (isTypeMismatch(e)) {
				return null;
			}
			throw e;
		}
	}

	private String tryRowGetString(long rowHandle, String fieldName) throws ch.ehi.openfgdb4j.OpenFgdbException {
		try {
			return conn.getApi().rowGetString(rowHandle, fieldName);
		} catch (ch.ehi.openfgdb4j.OpenFgdbException e) {
			if (isTypeMismatch(e)) {
				return null;
			}
			throw e;
		}
	}

	private byte[] tryRowGetGeometry(long rowHandle) throws ch.ehi.openfgdb4j.OpenFgdbException {
		try {
			return conn.getApi().rowGetGeometry(rowHandle);
		} catch (ch.ehi.openfgdb4j.OpenFgdbException e) {
			if (isTypeMismatch(e) || isNotFound(e)) {
				return null;
			}
			throw e;
		}
	}

	private static boolean isTypeMismatch(ch.ehi.openfgdb4j.OpenFgdbException ex) {
		return ex != null && ex.getErrorCode() == ch.ehi.openfgdb4j.OpenFgdb.OFGDB_ERR_INVALID_ARG;
	}

	private static boolean isNotFound(ch.ehi.openfgdb4j.OpenFgdbException ex) {
		return ex != null && ex.getErrorCode() == ch.ehi.openfgdb4j.OpenFgdb.OFGDB_ERR_NOT_FOUND;
	}

	private static String joinColumns(List<String> columns) {
		StringBuilder out = new StringBuilder();
		String sep = "";
		for (String column : columns) {
			out.append(sep).append(column);
			sep = ",";
		}
		return out.toString();
	}

	private static final class ColumnMetadata {
		final String fieldName;
		int jdbcType;
		String typeName;
		Integer columnSize;
		Integer decimalDigits;
		Integer numPrecRadix;
		Integer charOctetLength;
		boolean inferredBySampling;

		private ColumnMetadata(String fieldName, int jdbcType) {
			this.fieldName = fieldName;
			applyJdbcType(jdbcType, false);
		}

		static ColumnMetadata withDefaultType(String fieldName) {
			int jdbcType = Types.VARCHAR;
			if (OfgdbTypeUtil.isPrimaryKeyColumn(fieldName)) {
				jdbcType = Types.INTEGER;
			} else if (OfgdbTypeUtil.isLikelyGeometryColumn(fieldName)) {
				jdbcType = Types.VARBINARY;
			}
			return new ColumnMetadata(fieldName, jdbcType);
		}

		void applyJdbcType(int jdbcType, boolean fromSampling) {
			this.jdbcType = jdbcType;
			this.typeName = OfgdbTypeUtil.jdbcTypeName(jdbcType, fieldName);
			this.columnSize = OfgdbTypeUtil.defaultColumnSize(jdbcType);
			this.decimalDigits = OfgdbTypeUtil.defaultDecimalDigits(jdbcType);
			this.numPrecRadix = OfgdbTypeUtil.defaultRadix(jdbcType);
			this.charOctetLength = OfgdbTypeUtil.defaultCharOctetLength(jdbcType);
			this.inferredBySampling = fromSampling;
		}
	}

	private static Map<String, Object> typeInfoRow(String typeName, int jdbcType, Integer precision,
			Boolean caseSensitive, Boolean unsigned) {
		Map<String, Object> row = new LinkedHashMap<String, Object>();
		row.put("TYPE_NAME", typeName);
		row.put("DATA_TYPE", Integer.valueOf(jdbcType));
		row.put("PRECISION", precision);
		row.put("LITERAL_PREFIX", "'");
		row.put("LITERAL_SUFFIX", "'");
		row.put("CREATE_PARAMS", null);
		row.put("NULLABLE", Integer.valueOf(typeNullableUnknown));
		row.put("CASE_SENSITIVE", caseSensitive);
		row.put("SEARCHABLE", Integer.valueOf(typeSearchable));
		row.put("UNSIGNED_ATTRIBUTE", unsigned);
		row.put("FIXED_PREC_SCALE", Boolean.FALSE);
		row.put("AUTO_INCREMENT", Boolean.FALSE);
		row.put("LOCAL_TYPE_NAME", typeName);
		row.put("MINIMUM_SCALE", Integer.valueOf(0));
		row.put("MAXIMUM_SCALE", Integer.valueOf(0));
		row.put("SQL_DATA_TYPE", null);
		row.put("SQL_DATETIME_SUB", null);
		row.put("NUM_PREC_RADIX", Integer.valueOf(10));
		return row;
	}

	private static boolean matchesPattern(String value,String jdbcPattern){
		if(jdbcPattern==null || "%".equals(jdbcPattern)){
			return true;
		}
		String normalizedPattern=jdbcPattern.replace("%", ".*").replace("_", ".");
		return value!=null && value.matches("(?i)"+normalizedPattern);
	}

	private static boolean acceptsTableType(String[] types) {
		if (types == null || types.length == 0) {
			return true;
		}
		for (String type : types) {
			if ("TABLE".equalsIgnoreCase(type)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		List<String> columns = Arrays.asList("TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
				"CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE",
				"AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE",
				"SQL_DATETIME_SUB", "NUM_PREC_RADIX");
		rows.add(typeInfoRow("VARCHAR", Types.VARCHAR, Integer.valueOf(4000), Boolean.TRUE, Boolean.FALSE));
		rows.add(typeInfoRow("INTEGER", Types.INTEGER, Integer.valueOf(10), Boolean.FALSE, Boolean.FALSE));
		rows.add(typeInfoRow("BIGINT", Types.BIGINT, Integer.valueOf(19), Boolean.FALSE, Boolean.FALSE));
		rows.add(typeInfoRow("DOUBLE", Types.DOUBLE, Integer.valueOf(15), Boolean.FALSE, Boolean.FALSE));
		rows.add(typeInfoRow("GEOMETRY", Types.VARBINARY, Integer.valueOf(Integer.MAX_VALUE), Boolean.FALSE, Boolean.FALSE));
		rows.add(typeInfoRow("BLOB", Types.BLOB, Integer.valueOf(Integer.MAX_VALUE), Boolean.FALSE, Boolean.FALSE));
		return new OfgdbResultSet(rows, columns);
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		return emptyResultSet("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE");
	}

	@Override
	public String getURL() throws SQLException {
		return conn.getUrl();
	}

	@Override
	public String getUserName() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		return emptyResultSet("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
				"DECIMAL_DIGITS", "PSEUDO_COLUMN");
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		return level == Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return true;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return true;
	}
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}
	public ResultSet getPseudoColumns(String arg0, String arg1, String arg2,
			String arg3) throws SQLException {
		return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE",
				"DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE");
	}

}
