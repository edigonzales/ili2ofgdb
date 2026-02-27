package ch.ehi.ofgdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.sql.Types;
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
                        assertEquals(Types.INTEGER, cols.getInt("DATA_TYPE"));
                    }
                    if ("name".equalsIgnoreCase(col)) {
                        foundName = true;
                        assertEquals(Types.VARCHAR, cols.getInt("DATA_TYPE"));
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

    @Test
    public void infersColumnTypesFromSampledRows() throws Exception {
        Connection conn = null;
        Path root = null;
        try {
            conn = TestDbUtil.openTempConnection("ofgdb-meta-types-");
            root = TestDbUtil.extractRootFromConnection(conn);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE t_types(T_Id INTEGER PRIMARY KEY NOT NULL, score DOUBLE, label VARCHAR(40))");
                stmt.executeUpdate("INSERT INTO t_types(T_Id, score, label) VALUES (7, 3.5, 'ok')");
            }

            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet cols = md.getColumns(null, null, "t_types", "%")) {
                boolean foundId = false;
                boolean foundScore = false;
                boolean foundLabel = false;
                while (cols.next()) {
                    String col = cols.getString("COLUMN_NAME");
                    if ("T_Id".equalsIgnoreCase(col)) {
                        foundId = true;
                        assertEquals(Types.INTEGER, cols.getInt("DATA_TYPE"));
                    }
                    if ("score".equalsIgnoreCase(col)) {
                        foundScore = true;
                        assertEquals(Types.DOUBLE, cols.getInt("DATA_TYPE"));
                    }
                    if ("label".equalsIgnoreCase(col)) {
                        foundLabel = true;
                        assertEquals(Types.VARCHAR, cols.getInt("DATA_TYPE"));
                    }
                }
                assertTrue(foundId);
                assertTrue(foundScore);
                assertTrue(foundLabel);
            }

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT T_Id, score, label FROM t_types")) {
                assertTrue(rs.next());
                assertEquals(Types.INTEGER, rs.getMetaData().getColumnType(1));
                assertEquals(Types.DOUBLE, rs.getMetaData().getColumnType(2));
                assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(3));
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

    @Test
    public void marksIliBlobGeometryColumnsAsGeometry() throws Exception {
        Connection conn = null;
        Path root = null;
        try {
            conn = TestDbUtil.openTempConnection("ofgdb-meta-geom-");
            root = TestDbUtil.extractRootFromConnection(conn);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(
                        "CREATE TABLE t_geom(T_Id INTEGER PRIMARY KEY NOT NULL, geom BLOB, name VARCHAR(40))");
                stmt.executeUpdate(
                        "CREATE TABLE T_ILI2DB_COLUMN_PROP(tablename VARCHAR(255), subtype VARCHAR(255), columnname VARCHAR(255), tag VARCHAR(1024), setting VARCHAR(8000))");
                stmt.executeUpdate(
                        "INSERT INTO T_ILI2DB_COLUMN_PROP(tablename, subtype, columnname, tag, setting) VALUES ('t_geom', NULL, 'geom', 'ch.ehi.ili2db.typeKind', 'SURFACE')");
                stmt.executeUpdate(
                        "INSERT INTO t_geom(T_Id, geom, name) VALUES (1, '__OFGDB_BYTES_B64__:AQIDBAU=', 'n1')");
            }

            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet cols = md.getColumns(null, null, "t_geom", "%")) {
                boolean foundGeom = false;
                while (cols.next()) {
                    if ("geom".equalsIgnoreCase(cols.getString("COLUMN_NAME"))) {
                        foundGeom = true;
                        assertEquals(Types.VARBINARY, cols.getInt("DATA_TYPE"));
                        assertEquals("GEOMETRY", cols.getString("TYPE_NAME"));
                    }
                }
                assertTrue(foundGeom);
            }

            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT geom FROM t_geom")) {
                assertTrue(rs.next());
                assertEquals(Types.VARBINARY, rs.getMetaData().getColumnType(1));
                assertEquals("GEOMETRY", rs.getMetaData().getColumnTypeName(1));
                byte[] value = rs.getBytes(1);
                assertNotNull(value);
                assertTrue(value.length > 0);
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
