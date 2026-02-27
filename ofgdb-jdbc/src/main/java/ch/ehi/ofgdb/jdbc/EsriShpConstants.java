package ch.ehi.ofgdb.jdbc;

interface EsriShpConstants {
    int ShapeNull = 0;
    int ShapePoint = 1;
    int ShapePointM = 21;
    int ShapePointZM = 11;
    int ShapePointZ = 9;
    int ShapeMultiPoint = 8;
    int ShapeMultiPointM = 28;
    int ShapeMultiPointZM = 18;
    int ShapeMultiPointZ = 20;
    int ShapePolyline = 3;
    int ShapePolylineM = 23;
    int ShapePolylineZM = 13;
    int ShapePolylineZ = 10;
    int ShapePolygon = 5;
    int ShapePolygonM = 25;
    int ShapePolygonZM = 15;
    int ShapePolygonZ = 19;
    int ShapeMultiPatchM = 31;
    int ShapeMultiPatch = 32;
    int ShapeGeneralPolyline = 50;
    int ShapeGeneralPolygon = 51;
    int ShapeGeneralPoint = 52;
    int ShapeGeneralMultiPoint = 53;
    int ShapeGeneralMultiPatch = 54;

    int shapeHasZs = -2147483648;
    int shapeHasMs = 1073741824;
    int shapeHasCurves = 536870912;
    int shapeHasIDs = 268435456;
    int shapeHasNormals = 134217728;
    int shapeHasTextures = 67108864;
    int shapeHasPartIDs = 33554432;
    int shapeHasMaterials = 16777216;
    int shapeIsCompressed = 8388608;
    int shapeModifierMask = -16777216;
    int shapeMultiPatchModifierMask = 15728640;
    int shapeBasicTypeMask = 255;
    int shapeBasicModifierMask = -1073741824;
    int shapeNonBasicModifierMask = 1056964608;
    int shapeExtendedModifierMask = -587202560;

    int segmentLine = 1;
    int segmentArc = 2;
    int segmentSpiral = 3;
    int segmentBezier3Curve = 4;
    int segmentEllipticArc = 5;

    int arcIsEmpty = 1;
    int arcIsLine = 2;
    int arcIsPoint = 4;
    int arcDefinedIP = 8;
    int arcIsCCW = 16;
    int arcIsMinor = 32;
}
