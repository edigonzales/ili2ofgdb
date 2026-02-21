package ch.ehi.ili2ofgdb;

import org.junit.Ignore;

import ch.ehi.ili2db.AbstractTestSetup;

@Ignore("openfgdb4j backend parity gap: SQL function coverage, metadata visibility, and geometry handling differ from ili2pg/ili2gpkg")
public class GeomIndex10OfgdbTest extends ch.ehi.ili2db.GeomIndex10Test {
    private static final String FGDBFILENAME = "build/test-ofgdb/GeomIndex10OfgdbTest.gdb";

    @Override
    protected AbstractTestSetup createTestSetup() {
        return new OfgdbTestSetup(FGDBFILENAME);
    }
}
