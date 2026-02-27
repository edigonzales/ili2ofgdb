# ofgdb-jdbc

Standalone JDBC driver for ESRI File Geodatabase based on `openfgdb4j`.

## Coordinates

- Group: `ch.so.agi`
- Artifact: `ofgdb-jdbc`

## JDBC

- Driver class: `ch.ehi.ofgdb.jdbc.OfgdbDriver`
- URL format: `jdbc:ofgdb:/absolute/path/to/database.gdb`

## DBeaver Setup

### Option A: File-based (local jar)

1. Open `Database` -> `Driver Manager` -> `New`.
2. Name the driver `ofgdb-jdbc`.
3. Set Class Name to `ch.ehi.ofgdb.jdbc.OfgdbDriver`.
4. Add library `build/libs/ofgdb-jdbc-<version>-all.jar`.
5. Save the driver.
6. Create a new connection with URL:
   `jdbc:ofgdb:/absolute/path/to/your.gdb`
7. Leave user/password empty.

### Option B: Maven-based (snapshot repository)

1. Open `Database` -> `Driver Manager` -> `New`.
2. Name the driver `ofgdb-jdbc`.
3. Set Class Name to `ch.ehi.ofgdb.jdbc.OfgdbDriver`.
4. In the driver libraries, add Maven artifact:
   `ch.so.agi:ofgdb-jdbc:0.1.0-SNAPSHOT`
   (optional explicit fat-jar classifier: `ch.so.agi:ofgdb-jdbc:0.1.0-SNAPSHOT:all`)
   If you use the artifact dialog fields, keep classifier empty for the default artifact.
5. Ensure DBeaver can access the snapshot repository:
   `https://jars.sogeo.services/snapshots`
6. Save the driver.
7. Create a new connection with URL:
   `jdbc:ofgdb:/absolute/path/to/your.gdb`
8. Leave user/password empty.

Important:

- Do not use version `RELEASE` for this artifact.
- The artifact must already be published to the snapshot repository (for example via GitHub Action `ofgdb-jdbc-fatjar` on `main` or via `./gradlew publishOfgdbJdbcFatJarSnapshot`).

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
