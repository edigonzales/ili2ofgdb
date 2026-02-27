package ch.ehi.ofgdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class OfgdbMetaDataTest {
    @Test
    public void exposesTableColumnsPrimaryKeyAndIndexMetadata() throws Exception {
        Connection conn = null;
        Path root = null;
        try {
            conn = TestDbUtil.openTempConnection("ofgdb-meta-");
            root = TestDbUtil.extractRootFromConnection(conn);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE t_meta(T_Id INTEGER PRIMARY KEY NOT NULL, name VARCHAR(100))");
            }

            DatabaseMetaData md = conn.getMetaData();
            assertNotNull(md.getIdentifierQuoteString());

            try (ResultSet tables = md.getTables(null, null, "t_meta", new String[] { "TABLE" })) {
                assertTrue(tables.next());
                assertEquals("t_meta", tables.getString("TABLE_NAME"));
            }

            try (ResultSet cols = md.getColumns(null, null, "t_meta", "%")) {
                boolean foundId = false;
                boolean foundName = false;
                while (cols.next()) {
                    String col = cols.getString("COLUMN_NAME");
                    if ("T_Id".equalsIgnoreCase(col) || "T_ID".equalsIgnoreCase(col)) {
                        foundId = true;
                    }
                    if ("name".equalsIgnoreCase(col)) {
                        foundName = true;
                    }
                }
                assertTrue(foundId);
                assertTrue(foundName);
            }

            try (ResultSet pk = md.getPrimaryKeys(null, null, "t_meta")) {
                assertTrue(pk.next());
                assertEquals("t_meta", pk.getString("TABLE_NAME"));
                assertTrue("T_Id".equalsIgnoreCase(pk.getString("COLUMN_NAME")));
            }

            try (ResultSet idx = md.getIndexInfo(null, null, "t_meta", true, false)) {
                assertTrue(idx.next());
                assertEquals("t_meta", idx.getString("TABLE_NAME"));
                assertTrue("T_Id".equalsIgnoreCase(idx.getString("COLUMN_NAME")));
            }

            try (ResultSet tableTypes = md.getTableTypes()) {
                assertTrue(tableTypes.next());
                assertEquals("TABLE", tableTypes.getString("TABLE_TYPE"));
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (root != null) {
                TestDbUtil.deleteRecursively(root);
            }
        }
    }
}
