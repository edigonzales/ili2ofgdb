package ch.ehi.ofgdb.jdbc;

import java.sql.Types;

final class OfgdbTypeUtil {
    private OfgdbTypeUtil() {
    }

    static boolean isPrimaryKeyColumn(String fieldName) {
        if (fieldName == null) {
            return false;
        }
        return "OBJECTID".equalsIgnoreCase(fieldName)
                || "OID".equalsIgnoreCase(fieldName)
                || "FID".equalsIgnoreCase(fieldName)
                || "T_ID".equalsIgnoreCase(fieldName)
                || "T_Id".equalsIgnoreCase(fieldName);
    }

    static boolean isLikelyGeometryColumn(String fieldName) {
        if (fieldName == null) {
            return false;
        }
        String normalized = fieldName.trim().toLowerCase(java.util.Locale.ROOT);
        return "shape".equals(normalized)
                || "geometry".equals(normalized)
                || "geometrie".equals(normalized)
                || "wkb_geometry".equals(normalized)
                || normalized.endsWith("_shape")
                || normalized.endsWith("_geom")
                || normalized.endsWith("_geometry")
                || normalized.endsWith("_geometrie");
    }

    static boolean looksBinaryText(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        int controlChars = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\u0000') {
                return true;
            }
            if (Character.isISOControl(c) && !Character.isWhitespace(c)) {
                controlChars++;
            }
        }
        return controlChars > 0 && controlChars * 4 >= value.length();
    }

    static int jdbcTypeFromValue(Object value) {
        if (value == null) {
            return Types.VARCHAR;
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return Types.INTEGER;
        }
        if (value instanceof Long) {
            return Types.BIGINT;
        }
        if (value instanceof Float || value instanceof Double) {
            return Types.DOUBLE;
        }
        if (value instanceof java.math.BigDecimal) {
            return Types.DECIMAL;
        }
        if (value instanceof Boolean) {
            return Types.BOOLEAN;
        }
        if (value instanceof byte[]) {
            return Types.VARBINARY;
        }
        if (value instanceof java.sql.Timestamp) {
            return Types.TIMESTAMP;
        }
        if (value instanceof java.sql.Date) {
            return Types.DATE;
        }
        if (value instanceof java.sql.Time) {
            return Types.TIME;
        }
        return Types.VARCHAR;
    }

    static String jdbcTypeName(int jdbcType, String columnName) {
        if ((jdbcType == Types.BLOB
                || jdbcType == Types.BINARY
                || jdbcType == Types.VARBINARY
                || jdbcType == Types.LONGVARBINARY)
                && isLikelyGeometryColumn(columnName)) {
            return "GEOMETRY";
        }
        switch (jdbcType) {
        case Types.SMALLINT:
            return "SMALLINT";
        case Types.INTEGER:
            return "INTEGER";
        case Types.BIGINT:
            return "BIGINT";
        case Types.REAL:
            return "REAL";
        case Types.FLOAT:
            return "FLOAT";
        case Types.DOUBLE:
            return "DOUBLE";
        case Types.DECIMAL:
            return "DECIMAL";
        case Types.NUMERIC:
            return "NUMERIC";
        case Types.BOOLEAN:
            return "BOOLEAN";
        case Types.DATE:
            return "DATE";
        case Types.TIME:
            return "TIME";
        case Types.TIMESTAMP:
            return "TIMESTAMP";
        case Types.BINARY:
            return "BINARY";
        case Types.VARBINARY:
            return "VARBINARY";
        case Types.LONGVARBINARY:
            return "LONGVARBINARY";
        case Types.BLOB:
            return "BLOB";
        default:
            return "VARCHAR";
        }
    }

    static Integer defaultColumnSize(int jdbcType) {
        switch (jdbcType) {
        case Types.SMALLINT:
            return Integer.valueOf(5);
        case Types.INTEGER:
            return Integer.valueOf(10);
        case Types.BIGINT:
            return Integer.valueOf(19);
        case Types.REAL:
            return Integer.valueOf(7);
        case Types.FLOAT:
        case Types.DOUBLE:
            return Integer.valueOf(15);
        case Types.DECIMAL:
        case Types.NUMERIC:
            return Integer.valueOf(38);
        case Types.BOOLEAN:
            return Integer.valueOf(1);
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BLOB:
            return Integer.valueOf(Integer.MAX_VALUE);
        case Types.DATE:
            return Integer.valueOf(10);
        case Types.TIME:
            return Integer.valueOf(8);
        case Types.TIMESTAMP:
            return Integer.valueOf(26);
        default:
            return Integer.valueOf(4000);
        }
    }

    static Integer defaultDecimalDigits(int jdbcType) {
        if (jdbcType == Types.DOUBLE) {
            return Integer.valueOf(15);
        }
        if (jdbcType == Types.REAL || jdbcType == Types.FLOAT) {
            return Integer.valueOf(7);
        }
        if (jdbcType == Types.DECIMAL) {
            return Integer.valueOf(10);
        }
        return Integer.valueOf(0);
    }

    static Integer defaultRadix(int jdbcType) {
        switch (jdbcType) {
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
        case Types.DECIMAL:
        case Types.NUMERIC:
            return Integer.valueOf(10);
        default:
            return null;
        }
    }

    static Integer defaultCharOctetLength(int jdbcType) {
        if (jdbcType == Types.VARCHAR) {
            return Integer.valueOf(4000);
        }
        if (jdbcType == Types.BINARY || jdbcType == Types.VARBINARY || jdbcType == Types.LONGVARBINARY) {
            return Integer.valueOf(Integer.MAX_VALUE);
        }
        return null;
    }
}
