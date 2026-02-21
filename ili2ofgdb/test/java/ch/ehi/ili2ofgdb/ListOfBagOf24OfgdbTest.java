package ch.ehi.ili2ofgdb;

import org.junit.Ignore;

import ch.ehi.ili2db.AbstractTestSetup;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@Ignore("openfgdb4j backend parity gap: SQL function coverage, metadata visibility, and geometry handling differ from ili2pg/ili2gpkg")
public class ListOfBagOf24OfgdbTest extends ch.ehi.ili2db.ListOfBagOf24Test {
    private static final String FGDBFILENAME = "build/test-ofgdb/ListOfBagOf24OfgdbTest.gdb";

    @Override
    protected AbstractTestSetup createTestSetup() {
        return new OfgdbTestSetup(FGDBFILENAME);
    }

    @Override
    protected void assertTableContainsColumns(Connection jdbcConnection, String tableName, String... expectedColumns) throws SQLException {
        ResultSet resultSet = null;
        try {
            resultSet = jdbcConnection.getMetaData().getColumns(null, null, tableName, null);
            List<String> actualValues = new ArrayList<String>();
            while (resultSet.next()) {
                actualValues.add(resultSet.getString("COLUMN_NAME"));
            }
            assertThat(actualValues, containsInAnyOrder(expectedColumns));
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    @Override
    protected void assertClassA1Attr8(Connection jdbcConnection) throws Exception {
        Statement statement = null;
        try {
            statement = jdbcConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT attr8 FROM " + setup.prefixName("classa1"));
            Ofgdb2iox ofgdb2iox = new Ofgdb2iox();
            while (resultSet.next()) {
                Object value = resultSet.getObject(1);
                if (!(value instanceof byte[])) {
                    fail("expected binary value in geometry column but got " + (value == null ? "null" : value.getClass().getName()));
                }
                assertEquals("COORD {C1 480000.0, C2 70000.0}", ofgdb2iox.read((byte[]) value).toString());
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    @Override
    protected void assertClassA1Attr9(Connection jdbcConnection) throws Exception {
        Statement statement = null;
        try {
            statement = jdbcConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT attr9 FROM " + setup.prefixName("classa1_attr9"));
            Ofgdb2iox ofgdb2iox = new Ofgdb2iox();
            while (resultSet.next()) {
                Object value = resultSet.getObject(1);
                if (!(value instanceof byte[])) {
                    fail("expected binary value in geometry column but got " + (value == null ? "null" : value.getClass().getName()));
                }
                assertEquals("COORD {C1 500000.0, C2 72000.0}", ofgdb2iox.read((byte[]) value).toString());
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }
}
