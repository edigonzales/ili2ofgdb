package ch.ehi.ili2ofgdb;

import org.junit.Ignore;

import ch.ehi.ili2db.AbstractTestSetup;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore("openfgdb4j backend parity gap: SQL function coverage, metadata visibility, and geometry handling differ from ili2pg/ili2gpkg")
public class MultiSurfaceArea24OfgdbTest extends ch.ehi.ili2db.MultiSurfaceArea24Test {
    private static final String FGDBFILENAME = "build/test-ofgdb/MultiSurfaceArea24OfgdbTest.gdb";

    @Override
    protected AbstractTestSetup createTestSetup() {
        return new OfgdbTestSetup(FGDBFILENAME);
    }

    @Override
    protected void assertMultiSurfaceArea24_classa12_geomattr(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT geomattr1 FROM " + setup.prefixName("classa1"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        while (rs.next()) {
            Object value = rs.getObject(1);
            if (!(value instanceof byte[])) {
                fail("expected binary value in geometry column but got " + (value == null ? "null" : value.getClass().getName()));
            }
            assertEquals("MULTISURFACE {surface [SURFACE {boundary [BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 480000.111, C2 70000.111, C3 5000.111}, COORD {C1 480000.999, C2 70000.111, C3 5000.111}, COORD {C1 480000.999, C2 70000.999, C3 5000.111}, COORD {C1 480000.111, C2 70000.111, C3 5000.111}]}}}, BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 480000.555, C2 70000.222, C3 5000.111}, COORD {C1 480000.666, C2 70000.222, C3 5000.111}, COORD {C1 480000.666, C2 70000.666, C3 5000.111}, COORD {C1 480000.555, C2 70000.222, C3 5000.111}]}}}]}, SURFACE {boundary [BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 490000.111, C2 70000.111, C3 5000.111}, COORD {C1 490000.999, C2 70000.111, C3 5000.111}, COORD {C1 490000.999, C2 70000.999, C3 5000.111}, COORD {C1 490000.111, C2 70000.111, C3 5000.111}]}}}, BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 490000.555, C2 70000.222, C3 5000.111}, COORD {C1 490000.666, C2 70000.222, C3 5000.111}, COORD {C1 490000.666, C2 70000.666, C3 5000.111}, COORD {C1 490000.555, C2 70000.222, C3 5000.111}]}}}]}]}", ofgdb2iox.read((byte[]) value).toString());
        }

        rs = stmt.executeQuery("SELECT geomattr1 FROM " + setup.prefixName("classa2"));
        while (rs.next()) {
            Object value = rs.getObject(1);
            if (!(value instanceof byte[])) {
                fail("expected binary value in geometry column but got " + (value == null ? "null" : value.getClass().getName()));
            }
            assertEquals("MULTISURFACE {surface [SURFACE {boundary [BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 480000.111, C2 70000.111, C3 5000.111}, COORD {C1 480000.999, C2 70000.111, C3 5000.111}, COORD {C1 480000.999, C2 70000.999, C3 5000.111}, COORD {C1 480000.111, C2 70000.111, C3 5000.111}]}}}, BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 480000.555, C2 70000.222, C3 5000.111}, COORD {C1 480000.666, C2 70000.222, C3 5000.111}, COORD {C1 480000.666, C2 70000.666, C3 5000.111}, COORD {C1 480000.555, C2 70000.222, C3 5000.111}]}}}]}, SURFACE {boundary [BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 490000.111, C2 70000.111, C3 5000.111}, COORD {C1 490000.999, C2 70000.111, C3 5000.111}, COORD {C1 490000.999, C2 70000.999, C3 5000.111}, COORD {C1 490000.111, C2 70000.111, C3 5000.111}]}}}, BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 490000.555, C2 70000.222, C3 5000.111}, COORD {C1 490000.666, C2 70000.222, C3 5000.111}, COORD {C1 490000.666, C2 70000.666, C3 5000.111}, COORD {C1 490000.555, C2 70000.222, C3 5000.111}]}}}]}]}", ofgdb2iox.read((byte[]) value).toString());
        }
    }
}
