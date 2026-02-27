package ch.ehi.ofgdb.jdbc;

import java.sql.Types;

final class OfgdbColumnSchema {
    enum GeometryRole {
        NONE,
        FEATURE_GEOMETRY,
        ILI_BLOB_GEOMETRY;

        boolean isGeometry() {
            return this != NONE;
        }
    }

    final String name;
    String esriFieldType;
    int jdbcType;
    String jdbcTypeName;
    Integer columnSize;
    Integer decimalDigits;
    Integer numPrecRadix;
    Integer charOctetLength;
    boolean nullable;
    boolean primaryKey;
    boolean oidColumn;
    GeometryRole geometryRole = GeometryRole.NONE;
    String iliTypeKind;
    String iliGeomType;
    String iliSrid;
    String iliCoordDimension;

    OfgdbColumnSchema(String name) {
        this.name = name;
        this.jdbcType = OfgdbTypeUtil.isPrimaryKeyColumn(name) ? Types.INTEGER : Types.VARCHAR;
        this.jdbcTypeName = OfgdbTypeUtil.jdbcTypeName(this.jdbcType, name);
        this.columnSize = OfgdbTypeUtil.defaultColumnSize(this.jdbcType);
        this.decimalDigits = OfgdbTypeUtil.defaultDecimalDigits(this.jdbcType);
        this.numPrecRadix = OfgdbTypeUtil.defaultRadix(this.jdbcType);
        this.charOctetLength = OfgdbTypeUtil.defaultCharOctetLength(this.jdbcType);
        this.nullable = true;
        this.primaryKey = OfgdbTypeUtil.isPrimaryKeyColumn(name);
        this.oidColumn = "OBJECTID".equalsIgnoreCase(name) || "OID".equalsIgnoreCase(name);
    }

    void applyJdbcType(int jdbcType, String typeName, Integer columnSize, Integer decimalDigits, Integer numPrecRadix,
            Integer charOctetLength) {
        this.jdbcType = jdbcType;
        this.jdbcTypeName = typeName != null ? typeName : OfgdbTypeUtil.jdbcTypeName(jdbcType, name);
        this.columnSize = columnSize != null ? columnSize : OfgdbTypeUtil.defaultColumnSize(jdbcType);
        this.decimalDigits = decimalDigits != null ? decimalDigits : OfgdbTypeUtil.defaultDecimalDigits(jdbcType);
        this.numPrecRadix = numPrecRadix != null ? numPrecRadix : OfgdbTypeUtil.defaultRadix(jdbcType);
        this.charOctetLength = charOctetLength != null ? charOctetLength : OfgdbTypeUtil.defaultCharOctetLength(jdbcType);
    }

    void setGeometryRole(GeometryRole geometryRole) {
        this.geometryRole = geometryRole != null ? geometryRole : GeometryRole.NONE;
        if (this.geometryRole.isGeometry()) {
            applyJdbcType(
                    Types.VARBINARY,
                    "GEOMETRY",
                    Integer.valueOf(Integer.MAX_VALUE),
                    Integer.valueOf(0),
                    null,
                    Integer.valueOf(Integer.MAX_VALUE));
        }
    }
}
