package ch.ehi.ofgdb.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class OfgdbTableSchema {
    final String tableName;
    final List<OfgdbColumnSchema> columns;
    final String itemTypeUuid;
    final String oidFieldName;
    final String shapeFieldName;
    private final Map<String, OfgdbColumnSchema> byLowerName;

    OfgdbTableSchema(String tableName, List<OfgdbColumnSchema> columns, String itemTypeUuid, String oidFieldName,
            String shapeFieldName) {
        this.tableName = tableName;
        this.columns = columns != null ? Collections.unmodifiableList(new ArrayList<OfgdbColumnSchema>(columns))
                : Collections.<OfgdbColumnSchema>emptyList();
        this.itemTypeUuid = itemTypeUuid;
        this.oidFieldName = oidFieldName;
        this.shapeFieldName = shapeFieldName;
        LinkedHashMap<String, OfgdbColumnSchema> index = new LinkedHashMap<String, OfgdbColumnSchema>();
        for (OfgdbColumnSchema column : this.columns) {
            index.put(column.name.toLowerCase(Locale.ROOT), column);
        }
        this.byLowerName = Collections.unmodifiableMap(index);
    }

    OfgdbColumnSchema getColumn(String columnName) {
        if (columnName == null) {
            return null;
        }
        return byLowerName.get(columnName.toLowerCase(Locale.ROOT));
    }

    List<String> getPrimaryKeyColumns() {
        ArrayList<String> keys = new ArrayList<String>();
        for (OfgdbColumnSchema column : columns) {
            if (column.primaryKey || column.oidColumn) {
                keys.add(column.name);
            }
        }
        return keys;
    }

    List<Integer> jdbcTypesForColumns(List<String> columnNames) {
        ArrayList<Integer> out = new ArrayList<Integer>(columnNames.size());
        for (String columnName : columnNames) {
            OfgdbColumnSchema schema = getColumn(columnName);
            out.add(Integer.valueOf(schema != null ? schema.jdbcType : java.sql.Types.VARCHAR));
        }
        return out;
    }

    List<String> jdbcTypeNamesForColumns(List<String> columnNames) {
        ArrayList<String> out = new ArrayList<String>(columnNames.size());
        for (String columnName : columnNames) {
            OfgdbColumnSchema schema = getColumn(columnName);
            out.add(schema != null ? schema.jdbcTypeName : "VARCHAR");
        }
        return out;
    }
}
