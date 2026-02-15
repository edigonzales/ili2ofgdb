package ch.ehi.ili2ofgdb.jdbc;

/**
 * Backward-compatibility shim. Use OfgdbMetaData.
 */
@Deprecated
public class FgdbMetaData extends OfgdbMetaData {
    public FgdbMetaData(OfgdbConnection conn) {
        super(conn);
    }
}
