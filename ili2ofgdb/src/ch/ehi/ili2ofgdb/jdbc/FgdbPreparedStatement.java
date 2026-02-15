package ch.ehi.ili2ofgdb.jdbc;

/**
 * Backward-compatibility shim. Use OfgdbPreparedStatement.
 */
@Deprecated
public class FgdbPreparedStatement extends OfgdbPreparedStatement {
    protected FgdbPreparedStatement(OfgdbConnection conn, String sqlTemplate) {
        super(conn, sqlTemplate);
    }
}
