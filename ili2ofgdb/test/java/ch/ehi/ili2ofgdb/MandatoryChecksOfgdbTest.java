package ch.ehi.ili2ofgdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Test;

import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.gui.Config;

public class MandatoryChecksOfgdbTest {
    private static final String TEST_DATA_DIR = "test/data/MandatoryChecks";
    private static final String TEST_DB_DIR = "build/test-ofgdb";

    @Test
    public void mandatoryTextAndReferenceBecomeNotNullColumns() throws Exception {
        OfgdbTestSetup setup = new OfgdbTestSetup(TEST_DB_DIR + "/MandatorySimple.gdb");
        setup.resetDb();

        File data = new File(TEST_DATA_DIR + "/MandatorySimple.ili");
        Config config = setup.initConfig(data.getPath(), data.getPath() + ".log");
        Ili2db.setNoSmartMapping(config);
        config.setFunction(Config.FC_SCHEMAIMPORT);
        config.setCreateFk(Config.CREATE_FK_YES);
        config.setCreateMandatoryChecks(true);
        config.setTidHandling(Config.TID_HANDLING_PROPERTY);
        config.setBasketHandling(Config.BASKET_HANDLING_READWRITE);
        Ili2db.run(config, null);

        try (Connection jdbcConnection = setup.createConnection()) {
            assertColumn(jdbcConnection, setup.prefixName("classa"), "aname", Types.VARCHAR, "VARCHAR", 20, false);
            assertColumn(jdbcConnection, setup.prefixName("classa"), "target", Types.INTEGER, "INTEGER", 10, false);

            try (Statement stmt = jdbcConnection.createStatement()) {
                try {
                    stmt.executeUpdate("INSERT INTO " + setup.prefixName("classa") + " (T_Id) VALUES (1)");
                    fail("Expected NOT NULL violation for missing mandatory columns");
                } catch (SQLException expected) {
                    assertTrue(exceptionContains(expected, "not null") || exceptionContains(expected, "non-nullable"));
                }
            }
        }
    }

    private static void assertColumn(Connection jdbcConnection, String tableName, String columnName, int dataType,
            String typeName, int columnSize, boolean nullable) throws SQLException {
        try (ResultSet cols = jdbcConnection.getMetaData().getColumns(null, null, tableName, columnName)) {
            assertTrue("column not found: " + tableName + "." + columnName, cols.next());
            assertEquals(dataType, cols.getInt("DATA_TYPE"));
            assertEquals(typeName, cols.getString("TYPE_NAME"));
            assertEquals(columnSize, cols.getInt("COLUMN_SIZE"));
            assertEquals(nullable ? "YES" : "NO", cols.getString("IS_NULLABLE"));
            assertEquals(nullable ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls, cols.getInt("NULLABLE"));
        }
    }

    private static boolean exceptionContains(Throwable throwable, String text) {
        String lowercaseText = text.toLowerCase();
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.toLowerCase().contains(lowercaseText)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
