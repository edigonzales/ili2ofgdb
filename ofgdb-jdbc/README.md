# ofgdb-jdbc

Standalone JDBC driver for ESRI File Geodatabase based on `openfgdb4j`.

## Coordinates

- Group: `ch.so.agi`
- Artifact: `ofgdb-jdbc`

## JDBC

- Driver class: `ch.ehi.ofgdb.jdbc.OfgdbDriver`
- URL format: `jdbc:ofgdb:/absolute/path/to/database.gdb`

## DBeaver Setup

1. Open `Database` -> `Driver Manager` -> `New`.
2. Name the driver `ofgdb-jdbc`.
3. Set Class Name to `ch.ehi.ofgdb.jdbc.OfgdbDriver`.
4. Add library `build/libs/ofgdb-jdbc-<version>-all.jar`.
5. Save the driver.
6. Create a new connection with URL:
   `jdbc:ofgdb:/absolute/path/to/your.gdb`
7. Leave user/password empty.

## Build

```bash
./gradlew clean build verifyFatJarContainsNatives
```

The fat jar is produced at:

- `build/libs/ofgdb-jdbc-<version>-all.jar`

## Java

- Build target: Java 22+

## Snapshot Publish (Fat Jar)

Publish task:

```bash
./gradlew publishOfgdbJdbcFatJarSnapshot
```

Supported credentials/URL property names (same as main project):

- `sogeoSnapshotsUser`
- `sogeoSnapshotsPassword`
- `sogeoSnapshotsUrl`
