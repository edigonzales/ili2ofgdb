package ch.ehi.ofgdb.jdbc;

import java.nio.ByteOrder;
import ch.interlis.iom.IomObject;
import ch.interlis.iox_j.wkb.Iox2wkb;
import ch.interlis.iox_j.wkb.Iox2wkbException;
import com.vividsolutions.jts.io.ParseException;

final class OfgdbGeometryNormalizer {
    private static final double DEFAULT_ARC_STROKE_TOLERANCE = 0.001d;

    byte[] normalizeToWkb(byte[] value) throws Exception {
        if (value == null || value.length == 0) {
            return value;
        }
        if (EsriShapeSniffer.looksLikeWkb(value)) {
            return value;
        }
        if (!EsriShapeSniffer.looksLikeEsriShape(value)) {
            return value;
        }
        IomObject iomGeometry = parseEsriShape(value);
        if (iomGeometry == null) {
            return null;
        }
        return toWkb(iomGeometry, EsriShapeSniffer.hasZ(value));
    }

    private static IomObject parseEsriShape(byte[] value) throws ParseException, ch.interlis.iox.IoxException {
        return new OfgdbShape2iox().read(value);
    }

    private static byte[] toWkb(IomObject geometry, boolean hasZ) throws Iox2wkbException {
        Iox2wkb converter = new Iox2wkb(hasZ ? 3 : 2, ByteOrder.LITTLE_ENDIAN);
        String tag = geometry.getobjecttag();
        if ("COORD".equals(tag)) {
            return converter.coord2wkb(geometry);
        }
        if ("MULTICOORD".equals(tag)) {
            return converter.multicoord2wkb(geometry);
        }
        if ("POLYLINE".equals(tag)) {
            return converter.polyline2wkb(geometry, false, false, DEFAULT_ARC_STROKE_TOLERANCE);
        }
        if ("MULTIPOLYLINE".equals(tag)) {
            return converter.multiline2wkb(geometry, false, DEFAULT_ARC_STROKE_TOLERANCE);
        }
        if ("MULTISURFACE".equals(tag)) {
            return converter.multisurface2wkb(geometry, false, DEFAULT_ARC_STROKE_TOLERANCE, false);
        }
        if ("SURFACE".equals(tag)) {
            return converter.surface2wkb(geometry, false, DEFAULT_ARC_STROKE_TOLERANCE, false);
        }
        throw new Iox2wkbException("unsupported geometry object tag <" + tag + ">");
    }
}
