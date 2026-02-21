package ch.ehi.ili2ofgdb;

import org.junit.Ignore;

import ch.ehi.ili2db.AbstractTestSetup;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore("openfgdb4j backend parity gap: SQL function coverage, metadata visibility, and geometry handling differ from ili2pg/ili2gpkg")
public class MultiPolyline24OfgdbTest extends ch.ehi.ili2db.MultiPolyline24Test {
    private static final String FGDBFILENAME = "build/test-ofgdb/MultiPolyline24OfgdbTest.gdb";

    @Override
    protected AbstractTestSetup createTestSetup() {
        return new OfgdbTestSetup(FGDBFILENAME);
    }

    @Override
    protected void assertMultiPolyline24_classa1_geomattr1(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT geomattr1 FROM " + setup.prefixName("classa1"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        while (rs.next()) {
            Object value = rs.getObject(1);
            if (!(value instanceof byte[])) {
                fail("expected binary value in geometry column but got " + (value == null ? "null" : value.getClass().getName()));
            }
            assertEquals(
                    "MULTIPOLYLINE {polyline [POLYLINE {sequence SEGMENTS {segment [COORD {C1 480000.111, C2 70000.111, C3 4000.111}, COORD {C1 480000.222, C2 70000.222, C3 4000.222}, COORD {C1 480000.333, C2 70000.333, C3 4000.333}]}}, POLYLINE {sequence SEGMENTS {segment [COORD {C1 480000.444, C2 70000.444, C3 4000.444}, COORD {C1 480000.555, C2 70000.555, C3 4000.555}, COORD {C1 480000.666, C2 70000.666, C3 4000.666}]}}]}",
                    ofgdb2iox.read((byte[]) value).toString());
        }
    }
}
