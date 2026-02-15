package ch.ehi.ili2ofgdb.jdbc;

import ch.ehi.openfgdb4j.OpenFgdb;

/**
 * Backward-compatibility shim. Use OfgdbConnection.
 */
@Deprecated
public class FgdbConnection extends OfgdbConnection {
    protected FgdbConnection(OpenFgdb api, long dbHandle, String url) {
        super(api, dbHandle, url);
    }
}
