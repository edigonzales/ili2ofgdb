package ch.ehi.ili2ofgdb.jdbc;

/**
 * Backward-compatibility shim. Use OfgdbStatement.
 */
@Deprecated
public class FgdbStatement extends OfgdbStatement {
    protected FgdbStatement(OfgdbConnection conn) {
        super(conn);
    }
}
