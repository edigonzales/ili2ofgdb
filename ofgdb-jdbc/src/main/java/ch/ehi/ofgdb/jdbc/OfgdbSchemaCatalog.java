package ch.ehi.ofgdb.jdbc;

import java.io.StringReader;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import ch.ehi.openfgdb4j.OpenFgdb;
import ch.ehi.openfgdb4j.OpenFgdbException;

final class OfgdbSchemaCatalog {
    private static final String ITEM_TYPE_FEATURE_CLASS_UUID = "{70737809-852C-4A03-9E22-2CECEA5B9BFA}";

    private final OfgdbConnection conn;
    private final Map<String, OfgdbTableSchema> schemaCache = new LinkedHashMap<String, OfgdbTableSchema>();

    OfgdbSchemaCatalog(OfgdbConnection conn) {
        this.conn = conn;
    }

    synchronized OfgdbTableSchema getTableSchema(String tableName) throws SQLException {
        String resolvedTableName = conn.resolveTableName(tableName);
        String cacheKey = normalizeCacheKey(resolvedTableName);
        OfgdbTableSchema schema = schemaCache.get(cacheKey);
        if (schema != null) {
            return schema;
        }
        schema = loadTableSchema(resolvedTableName);
        schemaCache.put(cacheKey, schema);
        return schema;
    }

    synchronized void invalidateTable(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            return;
        }
        schemaCache.remove(normalizeCacheKey(tableName));
    }

    synchronized void invalidateAll() {
        schemaCache.clear();
    }

    private OfgdbTableSchema loadTableSchema(String tableName) throws SQLException {
        OpenFgdb api = conn.getApi();
        String resolvedTable = conn.resolveTableName(tableName);
        List<String> fieldNames = readTableFieldNames(api, resolvedTable);
        LinkedHashMap<String, OfgdbColumnSchema> columnsByLower = new LinkedHashMap<String, OfgdbColumnSchema>();
        for (String fieldName : fieldNames) {
            OfgdbColumnSchema schema = new OfgdbColumnSchema(fieldName);
            columnsByLower.put(fieldName.toLowerCase(Locale.ROOT), schema);
        }

        ParsedGdbItemDefinition definition = readGdbItemDefinition(api, resolvedTable);
        if (definition != null) {
            applyDefinition(columnsByLower, definition);
        } else {
            inferTypesBySampling(api, resolvedTable, columnsByLower, fieldNames);
        }
        applyIli2dbColumnProps(api, resolvedTable, columnsByLower);

        ArrayList<OfgdbColumnSchema> columns = new ArrayList<OfgdbColumnSchema>(fieldNames.size());
        for (String fieldName : fieldNames) {
            OfgdbColumnSchema column = columnsByLower.get(fieldName.toLowerCase(Locale.ROOT));
            if (column != null) {
                columns.add(column);
            }
        }
        return new OfgdbTableSchema(resolvedTable, columns, definition != null ? definition.itemTypeUuid : null,
                definition != null ? definition.oidFieldName : null,
                definition != null ? definition.shapeFieldName : null);
    }

    private static String normalizeCacheKey(String tableName) {
        return tableName.trim().toLowerCase(Locale.ROOT);
    }

    private List<String> readTableFieldNames(OpenFgdb api, String tableName) throws SQLException {
        long tableHandle = 0L;
        try {
            tableHandle = api.openTable(conn.getDbHandle(), tableName);
            return api.getFieldNames(tableHandle);
        } catch (OpenFgdbException e) {
            throw new SQLException("failed to read table fields for <" + tableName + ">", e);
        } finally {
            if (tableHandle != 0L) {
                try {
                    api.closeTable(conn.getDbHandle(), tableHandle);
                } catch (OpenFgdbException ignore) {
                }
            }
        }
    }

    private ParsedGdbItemDefinition readGdbItemDefinition(OpenFgdb api, String tableName) throws SQLException {
        String gdbItemsName = conn.resolveTableName("GDB_Items");
        long itemsTable = 0L;
        long cursor = 0L;
        try {
            itemsTable = api.openTable(conn.getDbHandle(), gdbItemsName);
            List<String> fieldNames = api.getFieldNames(itemsTable);
            String nameColumn = findColumnIgnoreCase(fieldNames, "Name");
            String definitionColumn = findColumnIgnoreCase(fieldNames, "Definition");
            String typeColumn = findColumnIgnoreCase(fieldNames, "Type");
            if (nameColumn == null || definitionColumn == null) {
                return null;
            }
            List<String> selectColumns = new ArrayList<String>();
            selectColumns.add(nameColumn);
            selectColumns.add(definitionColumn);
            if (typeColumn != null) {
                selectColumns.add(typeColumn);
            }
            cursor = api.search(itemsTable, joinColumns(selectColumns), "");
            while (true) {
                long rowHandle = api.fetchRow(cursor);
                if (rowHandle == 0L) {
                    break;
                }
                try {
                    String rowName = api.rowGetString(rowHandle, nameColumn);
                    if (rowName == null || !rowName.equalsIgnoreCase(tableName)) {
                        continue;
                    }
                    String definitionXml = api.rowGetString(rowHandle, definitionColumn);
                    String itemType = typeColumn != null ? api.rowGetString(rowHandle, typeColumn) : null;
                    ParsedGdbItemDefinition definition = parseDefinitionXml(definitionXml);
                    if (definition != null) {
                        definition.itemTypeUuid = itemType;
                        return definition;
                    }
                    return null;
                } finally {
                    api.closeRow(rowHandle);
                }
            }
            return null;
        } catch (OpenFgdbException e) {
            if (isNotFound(e)) {
                return null;
            }
            throw new SQLException("failed to read GDB_Items metadata for table <" + tableName + ">", e);
        } finally {
            if (cursor != 0L) {
                try {
                    api.closeCursor(cursor);
                } catch (OpenFgdbException ignore) {
                }
            }
            if (itemsTable != 0L) {
                try {
                    api.closeTable(conn.getDbHandle(), itemsTable);
                } catch (OpenFgdbException ignore) {
                }
            }
        }
    }

    private void applyDefinition(Map<String, OfgdbColumnSchema> columnsByLower, ParsedGdbItemDefinition definition) {
        if (definition == null) {
            return;
        }
        for (ParsedFieldDefinition field : definition.fields) {
            OfgdbColumnSchema column = columnsByLower.get(field.name.toLowerCase(Locale.ROOT));
            if (column == null) {
                continue;
            }
            column.esriFieldType = field.fieldType;
            if (field.nullable != null) {
                column.nullable = field.nullable.booleanValue();
            }
            if (definition.oidFieldName != null && definition.oidFieldName.equalsIgnoreCase(column.name)) {
                column.oidColumn = true;
                column.primaryKey = true;
            }
            applyEsriType(column, field.fieldType, field.length, field.precision, field.scale);
        }
        if (definition.shapeFieldName != null) {
            OfgdbColumnSchema shapeColumn = columnsByLower.get(definition.shapeFieldName.toLowerCase(Locale.ROOT));
            if (shapeColumn != null) {
                shapeColumn.setGeometryRole(OfgdbColumnSchema.GeometryRole.FEATURE_GEOMETRY);
            }
        }
        if (ITEM_TYPE_FEATURE_CLASS_UUID.equalsIgnoreCase(definition.itemTypeUuid) && definition.shapeFieldName == null) {
            for (OfgdbColumnSchema column : columnsByLower.values()) {
                if ("esriFieldTypeGeometry".equalsIgnoreCase(column.esriFieldType)) {
                    column.setGeometryRole(OfgdbColumnSchema.GeometryRole.FEATURE_GEOMETRY);
                }
            }
        }
    }

    private static void applyEsriType(OfgdbColumnSchema column, String esriFieldType, Integer length, Integer precision,
            Integer scale) {
        String normalized = esriFieldType != null ? esriFieldType.trim().toLowerCase(Locale.ROOT) : "";
        if ("esrifieldtypeoid".equals(normalized) || "esrifieldtypeinteger".equals(normalized)) {
            column.applyJdbcType(Types.INTEGER, "INTEGER", integerColumnSize(length), Integer.valueOf(0), Integer.valueOf(10), null);
            return;
        }
        if ("esrifieldtypesmallinteger".equals(normalized)) {
            column.applyJdbcType(Types.SMALLINT, "SMALLINT", Integer.valueOf(5), Integer.valueOf(0), Integer.valueOf(10), null);
            return;
        }
        if ("esrifieldtypedouble".equals(normalized)) {
            column.applyJdbcType(Types.DOUBLE, "DOUBLE", integerColumnSize(length), decimalDigits(scale),
                    Integer.valueOf(10), null);
            return;
        }
        if ("esrifieldtypesingle".equals(normalized)) {
            column.applyJdbcType(Types.REAL, "REAL", integerColumnSize(length), decimalDigits(scale), Integer.valueOf(10),
                    null);
            return;
        }
        if ("esrifieldtypedate".equals(normalized) || "esrifieldtypetimestampoffset".equals(normalized)) {
            column.applyJdbcType(Types.TIMESTAMP, "TIMESTAMP", Integer.valueOf(26), Integer.valueOf(0), null, null);
            return;
        }
        if ("esrifieldtypeblob".equals(normalized)) {
            column.applyJdbcType(Types.BLOB, "BLOB", Integer.valueOf(Integer.MAX_VALUE), Integer.valueOf(0), null, null);
            return;
        }
        if ("esrifieldtypegeometry".equals(normalized)) {
            column.setGeometryRole(OfgdbColumnSchema.GeometryRole.FEATURE_GEOMETRY);
            return;
        }
        if ("esrifieldtypestring".equals(normalized) || "esrifieldtypeguid".equals(normalized)
                || "esrifieldtypeglobalid".equals(normalized) || "esrifieldtypexml".equals(normalized)) {
            int size = (length != null && length.intValue() > 0) ? length.intValue() : 4000;
            column.applyJdbcType(Types.VARCHAR, "VARCHAR", Integer.valueOf(size), Integer.valueOf(0), null,
                    Integer.valueOf(size));
            return;
        }
        int fallbackSize = (length != null && length.intValue() > 0) ? length.intValue() : 4000;
        column.applyJdbcType(Types.VARCHAR, "VARCHAR", Integer.valueOf(fallbackSize), Integer.valueOf(0), null,
                Integer.valueOf(fallbackSize));
    }

    private static Integer decimalDigits(Integer scale) {
        if (scale != null) {
            return Integer.valueOf(Math.max(0, scale.intValue()));
        }
        return Integer.valueOf(0);
    }

    private static Integer integerColumnSize(Integer length) {
        if (length != null && length.intValue() > 0) {
            return length;
        }
        return Integer.valueOf(10);
    }

    private void applyIli2dbColumnProps(OpenFgdb api, String tableName, Map<String, OfgdbColumnSchema> columnsByLower)
            throws SQLException {
        String tablePropName = conn.resolveTableName("T_ILI2DB_COLUMN_PROP");
        long metaTable = 0L;
        long cursor = 0L;
        try {
            metaTable = api.openTable(conn.getDbHandle(), tablePropName);
            List<String> fieldNames = api.getFieldNames(metaTable);
            String tabNameCol = findColumnIgnoreCase(fieldNames, "tablename");
            String colNameCol = findColumnIgnoreCase(fieldNames, "columnname");
            String tagCol = findColumnIgnoreCase(fieldNames, "tag");
            String settingCol = findColumnIgnoreCase(fieldNames, "setting");
            if (tabNameCol == null || colNameCol == null || tagCol == null || settingCol == null) {
                return;
            }
            cursor = api.search(metaTable, joinColumns(Arrays.asList(tabNameCol, colNameCol, tagCol, settingCol)), "");
            while (true) {
                long rowHandle = api.fetchRow(cursor);
                if (rowHandle == 0L) {
                    break;
                }
                try {
                    String rowTable = api.rowGetString(rowHandle, tabNameCol);
                    if (rowTable == null || !rowTable.equalsIgnoreCase(tableName)) {
                        continue;
                    }
                    String rowColumn = api.rowGetString(rowHandle, colNameCol);
                    String rowTag = api.rowGetString(rowHandle, tagCol);
                    String rowSetting = api.rowGetString(rowHandle, settingCol);
                    if (rowColumn == null || rowTag == null) {
                        continue;
                    }
                    OfgdbColumnSchema column = columnsByLower.get(rowColumn.toLowerCase(Locale.ROOT));
                    if (column == null) {
                        continue;
                    }
                    String lowerTag = rowTag.toLowerCase(Locale.ROOT);
                    if (lowerTag.endsWith(".typekind")) {
                        column.iliTypeKind = rowSetting;
                        if (isGeometryTypeKind(rowSetting) && isBlobLike(column)) {
                            column.setGeometryRole(OfgdbColumnSchema.GeometryRole.ILI_BLOB_GEOMETRY);
                        }
                    } else if (lowerTag.endsWith(".geomtype")) {
                        column.iliGeomType = rowSetting;
                    } else if (lowerTag.endsWith(".srid")) {
                        column.iliSrid = rowSetting;
                    } else if (lowerTag.endsWith(".coorddimension")) {
                        column.iliCoordDimension = rowSetting;
                    }
                } finally {
                    api.closeRow(rowHandle);
                }
            }
        } catch (OpenFgdbException e) {
            if (isNotFound(e)) {
                return;
            }
            throw new SQLException("failed to read T_ILI2DB_COLUMN_PROP for table <" + tableName + ">", e);
        } finally {
            if (cursor != 0L) {
                try {
                    api.closeCursor(cursor);
                } catch (OpenFgdbException ignore) {
                }
            }
            if (metaTable != 0L) {
                try {
                    api.closeTable(conn.getDbHandle(), metaTable);
                } catch (OpenFgdbException ignore) {
                }
            }
        }
    }

    private static boolean isBlobLike(OfgdbColumnSchema column) {
        if (column == null) {
            return false;
        }
        if (column.jdbcType == Types.BLOB
                || column.jdbcType == Types.BINARY
                || column.jdbcType == Types.VARBINARY
                || column.jdbcType == Types.LONGVARBINARY) {
            return true;
        }
        return column.esriFieldType != null && "esriFieldTypeBlob".equalsIgnoreCase(column.esriFieldType);
    }

    private static boolean isGeometryTypeKind(String typeKind) {
        if (typeKind == null) {
            return false;
        }
        String normalized = typeKind.trim().toUpperCase(Locale.ROOT);
        return "COORD".equals(normalized)
                || "MULTICOORD".equals(normalized)
                || "POLYLINE".equals(normalized)
                || "MULTIPOLYLINE".equals(normalized)
                || "SURFACE".equals(normalized)
                || "AREA".equals(normalized)
                || "MULTISURFACE".equals(normalized)
                || "MULTIAREA".equals(normalized);
    }

    private void inferTypesBySampling(OpenFgdb api, String tableName, Map<String, OfgdbColumnSchema> columnsByLower,
            List<String> fieldNames) throws SQLException {
        if (fieldNames.isEmpty()) {
            return;
        }
        long tableHandle = 0L;
        long cursor = 0L;
        try {
            tableHandle = api.openTable(conn.getDbHandle(), tableName);
            cursor = api.search(tableHandle, joinColumns(fieldNames), "");
            int sampledRows = 0;
            while (sampledRows < 256) {
                long rowHandle = api.fetchRow(cursor);
                if (rowHandle == 0L) {
                    break;
                }
                sampledRows++;
                try {
                    for (String fieldName : fieldNames) {
                        OfgdbColumnSchema column = columnsByLower.get(fieldName.toLowerCase(Locale.ROOT));
                        if (column == null || column.jdbcType != Types.VARCHAR || column.geometryRole.isGeometry()) {
                            continue;
                        }
                        Integer inferredType = tryInferJdbcTypeFromRow(api, rowHandle, fieldName);
                        if (inferredType != null) {
                            column.applyJdbcType(inferredType.intValue(), null, null, null, null, null);
                        }
                    }
                } finally {
                    api.closeRow(rowHandle);
                }
            }
        } catch (OpenFgdbException e) {
            throw new SQLException("failed to infer table schema for <" + tableName + ">", e);
        } finally {
            if (cursor != 0L) {
                try {
                    api.closeCursor(cursor);
                } catch (OpenFgdbException ignore) {
                }
            }
            if (tableHandle != 0L) {
                try {
                    api.closeTable(conn.getDbHandle(), tableHandle);
                } catch (OpenFgdbException ignore) {
                }
            }
        }
    }

    private static Integer tryInferJdbcTypeFromRow(OpenFgdb api, long rowHandle, String fieldName) throws OpenFgdbException {
        if (api.rowIsNull(rowHandle, fieldName)) {
            return null;
        }
        try {
            api.rowGetBlob(rowHandle, fieldName);
            return Integer.valueOf(Types.VARBINARY);
        } catch (OpenFgdbException e) {
            if (!isTypeMismatch(e)) {
                throw e;
            }
        }
        try {
            api.rowGetInt32(rowHandle, fieldName);
            return Integer.valueOf(Types.INTEGER);
        } catch (OpenFgdbException e) {
            if (!isTypeMismatch(e)) {
                throw e;
            }
        }
        try {
            api.rowGetDouble(rowHandle, fieldName);
            return Integer.valueOf(Types.DOUBLE);
        } catch (OpenFgdbException e) {
            if (!isTypeMismatch(e)) {
                throw e;
            }
        }
        try {
            api.rowGetString(rowHandle, fieldName);
            return Integer.valueOf(Types.VARCHAR);
        } catch (OpenFgdbException e) {
            if (!isTypeMismatch(e)) {
                throw e;
            }
        }
        return null;
    }

    private static ParsedGdbItemDefinition parseDefinitionXml(String definitionXml) {
        if (definitionXml == null || definitionXml.trim().isEmpty()) {
            return null;
        }
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            configureSecureXmlFactory(factory);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(new StringReader(definitionXml)));
            ParsedGdbItemDefinition definition = new ParsedGdbItemDefinition();
            definition.oidFieldName = firstTagText(doc, "OIDFieldName");
            definition.shapeFieldName = firstTagText(doc, "ShapeFieldName");
            NodeList allNodes = doc.getElementsByTagName("*");
            for (int i = 0; i < allNodes.getLength(); i++) {
                Node node = allNodes.item(i);
                if (!(node instanceof Element) || !nodeNameMatches(node, "GPFieldInfoEx")) {
                    continue;
                }
                Element fieldNode = (Element) node;
                String fieldName = childTagText(fieldNode, "Name");
                if (fieldName == null || fieldName.trim().isEmpty()) {
                    continue;
                }
                ParsedFieldDefinition field = new ParsedFieldDefinition();
                field.name = fieldName;
                field.fieldType = childTagText(fieldNode, "FieldType");
                field.length = parsePositiveInt(childTagText(fieldNode, "Length"));
                field.precision = parsePositiveInt(childTagText(fieldNode, "Precision"));
                field.scale = parsePositiveInt(childTagText(fieldNode, "Scale"));
                field.nullable = parseNullable(childTagText(fieldNode, "IsNullable"));
                definition.fields.add(field);
            }
            if (definition.fields.isEmpty() && definition.oidFieldName == null && definition.shapeFieldName == null) {
                return null;
            }
            return definition;
        } catch (Exception ex) {
            return null;
        }
    }

    private static void configureSecureXmlFactory(DocumentBuilderFactory factory) {
        trySetFeature(factory, "http://apache.org/xml/features/disallow-doctype-decl", true);
        trySetFeature(factory, "http://xml.org/sax/features/external-general-entities", false);
        trySetFeature(factory, "http://xml.org/sax/features/external-parameter-entities", false);
        trySetFeature(factory, "http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
    }

    private static void trySetFeature(DocumentBuilderFactory factory, String featureName, boolean enabled) {
        try {
            factory.setFeature(featureName, enabled);
        } catch (ParserConfigurationException ignore) {
        }
    }

    private static String firstTagText(Document document, String tagName) {
        NodeList allNodes = document.getElementsByTagName("*");
        for (int i = 0; i < allNodes.getLength(); i++) {
            Node node = allNodes.item(i);
            if (nodeNameMatches(node, tagName)) {
                return trimToNull(node.getTextContent());
            }
        }
        return null;
    }

    private static String childTagText(Element parent, String tagName) {
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child instanceof Element && nodeNameMatches(child, tagName)) {
                return trimToNull(child.getTextContent());
            }
        }
        return null;
    }

    private static boolean nodeNameMatches(Node node, String localTagName) {
        if (node == null || localTagName == null) {
            return false;
        }
        String name = node.getNodeName();
        return name != null && name.endsWith(localTagName);
    }

    private static Integer parsePositiveInt(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Integer.valueOf(Integer.parseInt(value.trim()));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Boolean parseNullable(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(normalized) || "1".equals(normalized) || "yes".equals(normalized)) {
            return Boolean.TRUE;
        }
        if ("false".equals(normalized) || "0".equals(normalized) || "no".equals(normalized)) {
            return Boolean.FALSE;
        }
        return null;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String findColumnIgnoreCase(List<String> columns, String expected) {
        for (String candidate : columns) {
            if (candidate.equalsIgnoreCase(expected)) {
                return candidate;
            }
        }
        return null;
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

    private static boolean isTypeMismatch(OpenFgdbException ex) {
        return ex != null && ex.getErrorCode() == OpenFgdb.OFGDB_ERR_INVALID_ARG;
    }

    private static boolean isNotFound(OpenFgdbException ex) {
        return ex != null && ex.getErrorCode() == OpenFgdb.OFGDB_ERR_NOT_FOUND;
    }

    private static final class ParsedGdbItemDefinition {
        final List<ParsedFieldDefinition> fields = new ArrayList<ParsedFieldDefinition>();
        String oidFieldName;
        String shapeFieldName;
        String itemTypeUuid;
    }

    private static final class ParsedFieldDefinition {
        String name;
        String fieldType;
        Integer length;
        Integer precision;
        Integer scale;
        Boolean nullable;
    }
}
