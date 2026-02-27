package ch.ehi.ofgdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class OfgdbStatementExecuteTest {
    @Test
    public void executeDetectsMultilineSelectStatements() throws Exception {
        Connection conn = null;
        Path root = null;
        try {
            conn = TestDbUtil.openTempConnection("ofgdb-execute-select-");
            root = TestDbUtil.extractRootFromConnection(conn);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE t_exec(T_Id INTEGER PRIMARY KEY NOT NULL, name VARCHAR(40))");
                stmt.executeUpdate("INSERT INTO t_exec(T_Id, name) VALUES (1, 'n1')");
                stmt.executeUpdate("INSERT INTO t_exec(T_Id, name) VALUES (2, 'n2')");
            }

            try (Statement stmt = conn.createStatement()) {
                boolean hasResultSet = stmt.execute("SELECT\n  T_Id\nFROM\n  t_exec");
                assertTrue(hasResultSet);
                ResultSet rs = stmt.getResultSet();
                assertNotNull(rs);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
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
