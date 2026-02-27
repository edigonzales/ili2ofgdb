package ch.ehi.ofgdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

public class OfgdbPreparedStatement extends OfgdbStatement implements PreparedStatement {
    private final String sqlTemplate;
    private List<Object> params = new ArrayList<Object>();

    protected OfgdbPreparedStatement(OfgdbConnection conn, String sqlTemplate) {
        super(conn);
        this.sqlTemplate = sqlTemplate;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(materializeSql(sqlTemplate, params));
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(materializeSql(sqlTemplate, params));
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(materializeSql(sqlTemplate, params));
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return executeUpdate();
    }

    @Override
    public void clearParameters() {
        params.clear();
    }

    @Override
    public void addBatch() throws SQLException {
        queueBatchSql(materializeSql(sqlTemplate, params));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) {
        setParam(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        setParam(parameterIndex, Boolean.valueOf(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        setParam(parameterIndex, Byte.valueOf(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        setParam(parameterIndex, Short.valueOf(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        setParam(parameterIndex, Integer.valueOf(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        setParam(parameterIndex, Long.valueOf(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        setParam(parameterIndex, Float.valueOf(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        setParam(parameterIndex, Double.valueOf(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) {
        setParam(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() {
        try {
            javax.sql.rowset.RowSetMetaDataImpl md = new javax.sql.rowset.RowSetMetaDataImpl();
            List<String> columns = extractProjectedColumns(sqlTemplate);
            md.setColumnCount(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                md.setColumnName(i + 1, columns.get(i));
                md.setColumnType(i + 1, java.sql.Types.VARCHAR);
                md.setColumnTypeName(i + 1, "VARCHAR");
            }
            return md;
        } catch (SQLException e) {
            try {
                javax.sql.rowset.RowSetMetaDataImpl md = new javax.sql.rowset.RowSetMetaDataImpl();
                md.setColumnCount(0);
                return md;
            } catch (SQLException ignore) {
                return null;
            }
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        final int parameterCount = countParameters(sqlTemplate);
        return new ParameterMetaData() {
            @Override
            public int getParameterCount() {
                return parameterCount;
            }

            @Override
            public int isNullable(int param) {
                return ParameterMetaData.parameterNullableUnknown;
            }

            @Override
            public boolean isSigned(int param) {
                return true;
            }

            @Override
            public int getPrecision(int param) {
                return 0;
            }

            @Override
            public int getScale(int param) {
                return 0;
            }

            @Override
            public int getParameterType(int param) {
                return java.sql.Types.OTHER;
            }

            @Override
            public String getParameterTypeName(int param) {
                return "OTHER";
            }

            @Override
            public String getParameterClassName(int param) {
                return Object.class.getName();
            }

            @Override
            public int getParameterMode(int param) {
                return ParameterMetaData.parameterModeIn;
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                if (iface != null && iface.isAssignableFrom(getClass())) {
                    return iface.cast(this);
                }
                throw new SQLException("not a wrapper for " + (iface != null ? iface.getName() : "null"));
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) {
                return iface != null && iface.isAssignableFrom(getClass());
            }
        };
    }

    @Override
    public void setArray(int parameterIndex, Array x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) {
        setParam(parameterIndex, inputStream);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) {
        setParam(parameterIndex, inputStream);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) {
        setParam(parameterIndex, reader);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) {
        setParam(parameterIndex, reader);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) {
        setParam(parameterIndex, value);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) {
        setParam(parameterIndex, reader);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) {
        setParam(parameterIndex, reader);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) {
        setParam(parameterIndex, xmlObject);
    }

    @Override
    public void setURL(int parameterIndex, URL x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) {
        setParam(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) {
        setParam(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) {
        setParam(parameterIndex, value);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) {
        setParam(parameterIndex, reader);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) {
        setParam(parameterIndex, reader);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) {
        setParam(parameterIndex, reader);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
        setParam(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) {
        setParam(parameterIndex, null);
    }

    private void setParam(int parameterIndex, Object value) {
        int idx = parameterIndex - 1;
        while (params.size() <= idx) {
            params.add(null);
        }
        params.set(idx, value);
    }

    private static int countParameters(String sql) {
        if (sql == null || sql.isEmpty()) {
            return 0;
        }
        int count = 0;
        boolean inLiteral = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inLiteral && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inLiteral = !inLiteral;
                continue;
            }
            if (!inLiteral && c == '?') {
                count++;
            }
        }
        return count;
    }

    private static List<String> extractProjectedColumns(String sql) {
        List<String> columns = new ArrayList<String>();
        if (sql == null) {
            return columns;
        }
        String normalized = sql.trim();
        int selectPos = normalized.toUpperCase(Locale.ROOT).indexOf("SELECT ");
        int fromPos = normalized.toUpperCase(Locale.ROOT).indexOf(" FROM ");
        if (selectPos != 0 || fromPos < 0) {
            return columns;
        }
        String projection = normalized.substring("SELECT ".length(), fromPos).trim();
        if (projection.equals("*")) {
            return columns;
        }
        String[] parts = projection.split(",");
        for (String part : parts) {
            String column = part.trim();
            if (column.isEmpty()) {
                continue;
            }
            int asPos = column.toUpperCase(Locale.ROOT).indexOf(" AS ");
            if (asPos > 0) {
                column = column.substring(asPos + 4).trim();
            } else if (column.contains(".")) {
                column = column.substring(column.lastIndexOf('.') + 1).trim();
            }
            if (column.length() >= 2 && column.startsWith("\"") && column.endsWith("\"")) {
                column = column.substring(1, column.length() - 1);
            }
            columns.add(column);
        }
        return columns;
    }
}
