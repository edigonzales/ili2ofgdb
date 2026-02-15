package ch.ehi.ili2ofgdb;

/**
 * Backward-compatibility shim. Use OfgdbTestSetup.
 */
@Deprecated
public class FgdbTestSetup extends OfgdbTestSetup {
    public FgdbTestSetup() {
        super();
    }

    protected FgdbTestSetup(String fgdbFilename) {
        super(fgdbFilename);
    }
}
