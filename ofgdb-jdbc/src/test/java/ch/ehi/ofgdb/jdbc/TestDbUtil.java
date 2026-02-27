package ch.ehi.ofgdb.jdbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

final class TestDbUtil {
    private TestDbUtil() {
    }

    static Connection openTempConnection(String nameHint) throws Exception {
        Class.forName(OfgdbDriver.class.getName());
        Path root = Files.createTempDirectory(nameHint);
        Path dbPath = root.resolve("test.gdb");
        String url = OfgdbDriver.BASE_URL + dbPath.toAbsolutePath();
        return DriverManager.getConnection(url, null, null);
    }

    static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        Files.walk(path)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    static Path extractRootFromConnection(Connection conn) throws SQLException {
        String url = conn.getMetaData().getURL();
        String dbPath = url.substring(OfgdbDriver.BASE_URL.length());
        return Path.of(dbPath).getParent();
    }
}
