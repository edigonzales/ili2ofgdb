package ch.ehi.ofgdb.jdbc;

final class EsriShapeSniffer {
    private EsriShapeSniffer() {
    }

    static boolean looksLikeWkb(byte[] value) {
        if (value == null || value.length < 5) {
            return false;
        }
        int byteOrder = value[0] & 0xff;
        if (byteOrder != 0 && byteOrder != 1) {
            return false;
        }
        int type = readInt(value, 1, byteOrder == 1);
        int baseType = type & 0xffff;
        int normalizedBaseType = baseType % 1000;
        return normalizedBaseType >= 1 && normalizedBaseType <= 31;
    }

    static boolean looksLikeEsriShape(byte[] value) {
        if (value == null || value.length < 4) {
            return false;
        }
        int typeInt = readInt(value, 0, true);
        int geometryType = typeInt & EsriShpConstants.shapeBasicTypeMask;
        switch (geometryType) {
        case EsriShpConstants.ShapeNull:
        case EsriShpConstants.ShapePoint:
        case EsriShpConstants.ShapePointM:
        case EsriShpConstants.ShapePointZ:
        case EsriShpConstants.ShapePointZM:
        case EsriShpConstants.ShapeMultiPoint:
        case EsriShpConstants.ShapeMultiPointM:
        case EsriShpConstants.ShapeMultiPointZ:
        case EsriShpConstants.ShapeMultiPointZM:
        case EsriShpConstants.ShapePolyline:
        case EsriShpConstants.ShapePolylineM:
        case EsriShpConstants.ShapePolylineZ:
        case EsriShpConstants.ShapePolylineZM:
        case EsriShpConstants.ShapePolygon:
        case EsriShpConstants.ShapePolygonM:
        case EsriShpConstants.ShapePolygonZ:
        case EsriShpConstants.ShapePolygonZM:
        case EsriShpConstants.ShapeGeneralPoint:
        case EsriShpConstants.ShapeGeneralPolyline:
        case EsriShpConstants.ShapeGeneralPolygon:
        case EsriShpConstants.ShapeGeneralMultiPoint:
        case EsriShpConstants.ShapeGeneralMultiPatch:
            return true;
        default:
            return false;
        }
    }

    static boolean hasZ(byte[] value) {
        if (value == null || value.length < 4) {
            return false;
        }
        int typeInt = readInt(value, 0, true);
        int geometryType = typeInt & EsriShpConstants.shapeBasicTypeMask;
        return geometryType == EsriShpConstants.ShapePointZM
                || geometryType == EsriShpConstants.ShapePointZ
                || geometryType == EsriShpConstants.ShapeMultiPointZM
                || geometryType == EsriShpConstants.ShapeMultiPointZ
                || geometryType == EsriShpConstants.ShapePolylineZM
                || geometryType == EsriShpConstants.ShapePolylineZ
                || geometryType == EsriShpConstants.ShapePolygonZM
                || geometryType == EsriShpConstants.ShapePolygonZ
                || ((geometryType == EsriShpConstants.ShapeGeneralPoint
                        || geometryType == EsriShpConstants.ShapeGeneralPolyline
                        || geometryType == EsriShpConstants.ShapeGeneralPolygon
                        || geometryType == EsriShpConstants.ShapeGeneralMultiPoint
                        || geometryType == EsriShpConstants.ShapeGeneralMultiPatch)
                        && ((typeInt & EsriShpConstants.shapeHasZs) != 0));
    }

    private static int readInt(byte[] value, int offset, boolean littleEndian) {
        if (offset + 4 > value.length) {
            return 0;
        }
        if (littleEndian) {
            return (value[offset] & 0xff)
                    | ((value[offset + 1] & 0xff) << 8)
                    | ((value[offset + 2] & 0xff) << 16)
                    | ((value[offset + 3] & 0xff) << 24);
        }
        return ((value[offset] & 0xff) << 24)
                | ((value[offset + 1] & 0xff) << 16)
                | ((value[offset + 2] & 0xff) << 8)
                | (value[offset + 3] & 0xff);
    }
}
