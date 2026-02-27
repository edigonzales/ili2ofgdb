package ch.ehi.ofgdb.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OfgdbDriverUrlTest {
    @Test
    public void acceptsNewUrlPrefixOnly() throws Exception {
        OfgdbDriver driver = new OfgdbDriver();
        assertTrue(driver.acceptsURL("jdbc:ofgdb:/tmp/sample.gdb"));
        assertFalse(driver.acceptsURL("jdbc:ili2ofgdb:/tmp/sample.gdb"));
    }
}
