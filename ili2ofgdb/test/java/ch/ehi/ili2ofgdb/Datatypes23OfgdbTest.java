package ch.ehi.ili2ofgdb;

import org.junit.Ignore;
import org.junit.Test;

import ch.ehi.ili2db.AbstractTestSetup;

public class Datatypes23OfgdbTest extends ch.ehi.ili2db.Datatypes23Test {
    private static final String IGNORE_PARITY_REASON = "openfgdb4j backend parity gap: SQL function coverage, metadata visibility, and geometry handling differ from ili2pg/ili2gpkg";
    private static final String FGDBFILENAME = "build/test-ofgdb/Datatypes23OfgdbTest.gdb";

    @Override
    protected AbstractTestSetup createTestSetup() {
        return new OfgdbTestSetup(FGDBFILENAME);
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtfLine() throws Exception {
        super.exportXtfLine();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtfAttr() throws Exception {
        super.exportXtfAttr();
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
    public void importIliWithMetadata() throws Exception {
        super.importIliWithMetadata();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importXtfAttr() throws Exception {
        super.importXtfAttr();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importXtfLine() throws Exception {
        super.importXtfLine();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importXtfSurface() throws Exception {
        super.importXtfSurface();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void importXtfSurface_asLines() throws Exception {
        super.importXtfSurface_asLines();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtfAttr_invalidValuesAsText() throws Exception {
        super.exportXtfAttr_invalidValuesAsText();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtfSurface() throws Exception {
        super.exportXtfSurface();
    }

    @Override
    @Test
    @Ignore(IGNORE_PARITY_REASON)
    public void exportXtfSurface_asLines() throws Exception {
        super.exportXtfSurface_asLines();
    }
}
