package ch.ehi.ofgdb.jdbc.sql;

/**
 * Backward-compatibility shim. Use OfgdbSelectStmt.
 */
@Deprecated
public class FgdbSelectStmt extends OfgdbSelectStmt {

    public FgdbSelectStmt() {
        super();
    }
}
