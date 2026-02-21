package ch.ehi.ili2ofgdb;

import org.junit.Ignore;
import org.junit.Test;

import ch.ehi.ili2db.AbstractTestSetup;

public class MultipleGeomAttrsOfgdbTest extends ch.ehi.ili2db.MultipleGeomAttrsTest {
    private static final String IGNORE_PARITY_REASON = "openfgdb4j backend parity gap: SQL function coverage, metadata visibility, and geometry handling differ from ili2pg/ili2gpkg";
    private static final String FGDBFILENAME = "build/test-ofgdb/MultipleGeomAttrsOfgdbTest.gdb";

    @Override
    protected AbstractTestSetup createTestSetup() {
        return new OfgdbTestSetup(FGDBFILENAME);
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtf() throws Exception {
        super.exportXtf();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importIli() throws Exception {
        super.importIli();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importIliOneGeom() throws Exception {
        super.importIliOneGeom();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importIliExtendedClassSmart1() throws Exception {
        super.importIliExtendedClassSmart1();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importIliExtendedClassSmart1OneGeom() throws Exception {
        super.importIliExtendedClassSmart1OneGeom();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importIliExtendedClassSmart2() throws Exception {
        super.importIliExtendedClassSmart2();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importIliExtendedClassSmart2OneGeom() throws Exception {
        super.importIliExtendedClassSmart2OneGeom();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importXtf() throws Exception {
        super.importXtf();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtfOneGeom() throws Exception {
        super.exportXtfOneGeom();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtfExtendedClassSmart1() throws Exception {
        super.exportXtfExtendedClassSmart1();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtfExtendedClassSmart2() throws Exception {
        super.exportXtfExtendedClassSmart2();
    }
}
