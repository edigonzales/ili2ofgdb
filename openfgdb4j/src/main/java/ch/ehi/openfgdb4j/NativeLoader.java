package ch.ehi.openfgdb4j;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

public final class NativeLoader {
    private static volatile boolean loaded;

    private NativeLoader() {
    }

    public static synchronized void load() {
        if (loaded) {
            return;
        }
        ensureSupportedPlatform();

        String ext = detectExtension();
        List<File> candidates = new ArrayList<File>();

        String overridePath = System.getProperty("openfgdb4j.lib");
        if (overridePath != null && !overridePath.isEmpty()) {
            candidates.add(new File(overridePath));
        }
        String overrideEnv = System.getenv("OPENFGDB4J_LIB");
        if (overrideEnv != null && !overrideEnv.isEmpty()) {
            candidates.add(new File(overrideEnv));
        }

        candidates.add(new File("openfgdb4j/build/native/libopenfgdb" + ext));
        candidates.add(new File("openfgdb4j/build/native/Release/libopenfgdb" + ext));
        candidates.add(new File("build/native/libopenfgdb" + ext));
        candidates.add(new File("build/native/Release/libopenfgdb" + ext));
        candidates.addAll(resolveCodeSourceCandidates(ext));

        File selected = null;
        for (File candidate : candidates) {
            if (candidate.exists() && candidate.isFile()) {
                selected = candidate;
                break;
            }
        }

        if (selected == null) {
            throw new IllegalStateException(
                    "Native library not found for os="
                            + System.getProperty("os.name")
                            + ", arch="
                            + System.getProperty("os.arch")
                            + ". Looked in: "
                            + candidates);
        }

        System.load(selected.getAbsolutePath());
        loaded = true;
    }

    private static void ensureSupportedPlatform() {
        String os = System.getProperty("os.name", "").toLowerCase();
        String arch = System.getProperty("os.arch", "").toLowerCase();

        boolean isMac = os.contains("mac") || os.contains("darwin");
        boolean isArm64 = "aarch64".equals(arch) || "arm64".equals(arch);

        if (!isMac || !isArm64) {
            throw new IllegalStateException(
                    "openfgdb4j supports macOS arm64 only (detected os=" + os + ", arch=" + arch + ")");
        }
    }

    private static String detectExtension() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) {
            return ".dll";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return ".dylib";
        }
        return ".so";
    }

    private static List<File> resolveCodeSourceCandidates(String ext) {
        try {
            URI uri = NativeLoader.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            File codeSource = new File(uri);
            File baseDir = codeSource.isDirectory() ? codeSource : codeSource.getParentFile();
            if (baseDir == null) {
                return Collections.emptyList();
            }
            List<File> candidates = new ArrayList<File>();
            candidates.add(new File(baseDir, "libopenfgdb" + ext));
            candidates.add(new File(baseDir, "native/libopenfgdb" + ext));
            candidates.add(new File(baseDir, "../native/libopenfgdb" + ext));
            candidates.add(new File(baseDir, "../openfgdb4j/build/native/libopenfgdb" + ext));
            return candidates;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
}
