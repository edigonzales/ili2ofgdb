package ch.ehi.ili2ofgdb;

import org.junit.Ignore;

import ch.ehi.ili2db.AbstractTestSetup;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore("openfgdb4j backend parity gap: SQL function coverage, metadata visibility, and geometry handling differ from ili2pg/ili2gpkg")
public class GenericValueRanges24OfgdbTest extends ch.ehi.ili2db.GenericValueRanges24Test {
    private static final String FGDBFILENAME = "build/test-ofgdb/GenericValueRanges24OfgdbTest.gdb";

    @Override
    protected AbstractTestSetup createTestSetup() {
        return new OfgdbTestSetup(FGDBFILENAME);
    }

    @Override
    protected void assertAttrCoord(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT attrcoord FROM " + setup.prefixName("classa"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        assertTrue(rs.next());
        assertEquals("COORD {C1 2530001.0, C2 1150002.0}", geomToString(ofgdb2iox, rs));
        assertFalse(rs.next());
    }

    @Override
    protected void assertAttrMultiCoord(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT attrmulticoord FROM " + setup.prefixName("classa_attrmulticoord"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        assertTrue(rs.next());
        assertEquals("MULTICOORD {coord [COORD {C1 2530001.0, C2 1150002.0}, COORD {C1 2740003.0, C2 1260004.0}]}", geomToString(ofgdb2iox, rs));
        assertFalse(rs.next());
    }

    @Override
    protected void assertAttrLine(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT attrline FROM " + setup.prefixName("classa_attrline"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        assertTrue(rs.next());
        assertEquals("POLYLINE {sequence SEGMENTS {segment [COORD {C1 2480000.0, C2 1070000.0}, COORD {C1 2490000.0, C2 1080000.0}]}}", geomToString(ofgdb2iox, rs));
        assertFalse(rs.next());
    }

    @Override
    protected void assertAttrMultiLine(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT attrmultiline FROM " + setup.prefixName("classa_attrmultiline"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        assertTrue(rs.next());
        assertEquals("MULTIPOLYLINE {polyline [POLYLINE {sequence SEGMENTS {segment [COORD {C1 2480000.0, C2 1070000.0}, COORD {C1 2490000.0, C2 1080000.0}]}}, POLYLINE {sequence SEGMENTS {segment [COORD {C1 2480000.0, C2 1070000.0}, COORD {C1 2490000.0, C2 1080000.0}]}}]}", geomToString(ofgdb2iox, rs));
        assertFalse(rs.next());
    }

    @Override
    protected void assertAttrSurface(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT attrsurface FROM " + setup.prefixName("classa_attrsurface"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        assertTrue(rs.next());
        assertEquals("MULTISURFACE {surface SURFACE {boundary [BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 2480000.111, C2 1070000.111}, COORD {C1 2480000.999, C2 1070000.111}, COORD {C1 2480000.999, C2 1070000.999}, COORD {C1 2480000.111, C2 1070000.111}]}}}, BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 2480000.555, C2 1070000.222}, COORD {C1 2480000.666, C2 1070000.222}, COORD {C1 2480000.666, C2 1070000.666}, COORD {C1 2480000.555, C2 1070000.222}]}}}]}}", geomToString(ofgdb2iox, rs));
        assertFalse(rs.next());
    }

    @Override
    protected void assertAttrMultiSurface(Statement stmt) throws Exception {
        ResultSet rs = stmt.executeQuery("SELECT attrmultisurface FROM " + setup.prefixName("classa_attrmultisurface"));
        Ofgdb2iox ofgdb2iox = new Ofgdb2iox();

        assertTrue(rs.next());
        assertEquals("MULTISURFACE {surface SURFACE {boundary [BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 2480000.111, C2 1070000.111}, COORD {C1 2480000.999, C2 1070000.111}, COORD {C1 2480000.999, C2 1070000.999}, COORD {C1 2480000.111, C2 1070000.111}]}}}, BOUNDARY {polyline POLYLINE {sequence SEGMENTS {segment [COORD {C1 2480000.555, C2 1070000.222}, COORD {C1 2480000.666, C2 1070000.222}, COORD {C1 2480000.666, C2 1070000.666}, COORD {C1 2480000.555, C2 1070000.222}]}}}]}}", geomToString(ofgdb2iox, rs));
        assertFalse(rs.next());
    }

    private static String geomToString(Ofgdb2iox ofgdb2iox, ResultSet rs) throws Exception {
        Object value = rs.getObject(1);
        if (value == null) {
            return null;
        }
        if (!(value instanceof byte[])) {
            fail("expected binary value in geometry column but got " + value.getClass().getName());
        }
        return ofgdb2iox.read((byte[]) value).toString();
    }
}
