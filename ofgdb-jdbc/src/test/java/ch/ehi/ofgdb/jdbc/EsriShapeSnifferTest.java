package ch.ehi.ofgdb.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class EsriShapeSnifferTest {
    @Test
    public void detectsSimpleWkbPayload() {
        byte[] wkbPoint = new byte[] {
                1, // little endian
                1, 0, 0, 0, // wkbPoint
                0, 0, 0, 0, 0, 0, 0, 0, // x
                0, 0, 0, 0, 0, 0, 0, 0 // y
        };
        assertTrue(EsriShapeSniffer.looksLikeWkb(wkbPoint));
    }

    @Test
    public void detectsSimpleEsriShapePayload() {
        byte[] shapePoint = new byte[] {
                1, 0, 0, 0, // ShapePoint (little-endian int)
                0, 0, 0, 0, 0, 0, 0, 0, // x
                0, 0, 0, 0, 0, 0, 0, 0 // y
        };
        assertTrue(EsriShapeSniffer.looksLikeEsriShape(shapePoint));
        assertFalse(EsriShapeSniffer.looksLikeWkb(shapePoint));
        assertFalse(EsriShapeSniffer.hasZ(shapePoint));
    }
}
