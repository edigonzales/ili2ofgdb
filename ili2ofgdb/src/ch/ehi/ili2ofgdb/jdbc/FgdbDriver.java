package ch.ehi.ili2ofgdb.jdbc;

/**
 * Backward-compatibility shim. Use OfgdbDriver.
 */
@Deprecated
public class FgdbDriver extends OfgdbDriver {
    public static final String BASE_URL = OfgdbDriver.BASE_URL;
}
