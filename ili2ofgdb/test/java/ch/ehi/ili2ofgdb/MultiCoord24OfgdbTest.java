package ch.ehi.ili2ofgdb;

import org.junit.Ignore;

import ch.ehi.ili2db.AbstractTestSetup;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore("openfgdb4j backend parity gap: SQL function coverage, metadata visibility, and geometry handling differ from ili2pg/ili2gpkg")
public class MultiCoord24OfgdbTest extends ch.ehi.ili2db.MultiCoord24Test {
    private static final String FGDBFILENAME = "build/test-ofgdb/MultiCoord24OfgdbTest.gdb";

    @Override
    protected AbstractTestSetup createTestSetup() {
        return new OfgdbTestSetup(FGDBFILENAME);
    }

    @Override
    protected void assertMultiChoord24_classa1_geomattr1(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT geomattr1 FROM " + setup.prefixName("classa1"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        while (rs.next()) {
            Object value = rs.getObject(1);
            if (!(value instanceof byte[])) {
                fail("expected binary value in geometry column but got " + (value == null ? "null" : value.getClass().getName()));
            }
            assertEquals(
                    "MULTICOORD {coord [COORD {C1 2530001.0, C2 1150002.0}, COORD {C1 2740003.0, C2 1260004.0}]}",
                    ofgdb2iox.read((byte[]) value).toString());
        }
    }
}
