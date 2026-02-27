package ch.ehi.ofgdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class OfgdbStatementLimitTest {
    @Test
    public void limitOffsetSlicesRows() throws Exception {
        Connection conn = null;
        Path root = null;
        try {
            conn = TestDbUtil.openTempConnection("ofgdb-limit-");
            root = TestDbUtil.extractRootFromConnection(conn);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE t_limit(id INTEGER, name VARCHAR(40))");
                for (int i = 1; i <= 5; i++) {
                    stmt.executeUpdate("INSERT INTO t_limit(id, name) VALUES (" + i + ", 'n" + i + "')");
                }
            }

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id FROM t_limit ORDER BY id LIMIT 2 OFFSET 1")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
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
