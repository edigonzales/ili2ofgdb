package ch.ehi.ili2ofgdb.jdbc;

import java.util.List;
import java.util.Map;

/**
 * Backward-compatibility shim. Use OfgdbResultSet.
 */
@Deprecated
public class FgdbResultSet extends OfgdbResultSet {
    public FgdbResultSet(List<Map<String, Object>> rows, List<String> columns) {
        super(rows, columns);
    }
}
