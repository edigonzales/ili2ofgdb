package ch.ehi.openfgdb4j;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class NativeLoader {
    private static volatile boolean loaded;

    private NativeLoader() {
    }

    public static synchronized void load() {
        if (loaded) {
            return;
        }
        Platform platform = ensureSupportedPlatform();

        List<File> candidates = new ArrayList<File>();

        String overridePath = System.getProperty("openfgdb4j.lib");
        if (overridePath != null && !overridePath.isEmpty()) {
            candidates.add(new File(overridePath));
        }
        String overrideEnv = System.getenv("OPENFGDB4J_LIB");
        if (overrideEnv != null && !overrideEnv.isEmpty()) {
            candidates.add(new File(overrideEnv));
        }

        addBuildCandidates(candidates, platform.libraryNames);
        candidates.addAll(resolveCodeSourceCandidates(platform.libraryNames));

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
                            + platform.os
                            + ", arch="
                            + platform.arch
                            + ". Looked in: "
                            + candidates);
        }

        System.load(selected.getAbsolutePath());
        loaded = true;
    }

    private static void addBuildCandidates(List<File> candidates, List<String> libraryNames) {
        for (String libName : libraryNames) {
            candidates.add(new File("openfgdb4j/build/native/" + libName));
            candidates.add(new File("openfgdb4j/build/native/Release/" + libName));
            candidates.add(new File("build/native/" + libName));
            candidates.add(new File("build/native/Release/" + libName));
            candidates.add(new File("openfgdb4j/build/native/Debug/" + libName));
            candidates.add(new File("build/native/Debug/" + libName));
        }
    }

    private static Platform ensureSupportedPlatform() {
        String normalizedOs = normalizeOs(System.getProperty("os.name", ""));
        String normalizedArch = normalizeArch(System.getProperty("os.arch", ""));

        boolean supported =
                ("linux".equals(normalizedOs) && ("amd64".equals(normalizedArch) || "arm64".equals(normalizedArch)))
                        || ("macos".equals(normalizedOs) && ("amd64".equals(normalizedArch) || "arm64".equals(normalizedArch)))
                        || ("windows".equals(normalizedOs) && "amd64".equals(normalizedArch));

        if (!supported) {
            throw new IllegalStateException(
                    "openfgdb4j supports linux(amd64,arm64), macos(amd64,arm64), windows(amd64) "
                            + "(detected os="
                            + normalizedOs
                            + ", arch="
                            + normalizedArch
                            + ")");
        }

        String ext = detectExtension(normalizedOs);
        List<String> names = new ArrayList<String>();
        if ("windows".equals(normalizedOs)) {
            names.add("openfgdb" + ext);
            names.add("libopenfgdb" + ext);
        } else {
            names.add("libopenfgdb" + ext);
        }
        return new Platform(normalizedOs, normalizedArch, names);
    }

    private static String normalizeOs(String rawOs) {
        String os = rawOs.toLowerCase();
        if (os.contains("win")) {
            return "windows";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return "macos";
        }
        if (os.contains("nux") || os.contains("linux")) {
            return "linux";
        }
        return os;
    }

    private static String normalizeArch(String rawArch) {
        String arch = rawArch.toLowerCase();
        if ("x86_64".equals(arch) || "amd64".equals(arch)) {
            return "amd64";
        }
        if ("aarch64".equals(arch) || "arm64".equals(arch)) {
            return "arm64";
        }
        return arch;
    }

    private static String detectExtension(String os) {
        if ("windows".equals(os)) {
            return ".dll";
        }
        if ("macos".equals(os)) {
            return ".dylib";
        }
        return ".so";
    }

    private static List<File> resolveCodeSourceCandidates(List<String> libraryNames) {
        try {
            URI uri = NativeLoader.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            File codeSource = new File(uri);
            File baseDir = codeSource.isDirectory() ? codeSource : codeSource.getParentFile();
            if (baseDir == null) {
                return Collections.emptyList();
            }
            List<File> candidates = new ArrayList<File>();
            for (String libName : libraryNames) {
                candidates.add(new File(baseDir, libName));
                candidates.add(new File(baseDir, "native/" + libName));
                candidates.add(new File(baseDir, "../native/" + libName));
                candidates.add(new File(baseDir, "../libs/" + libName));
                candidates.add(new File(baseDir, "../openfgdb4j/build/native/" + libName));
            }
            return candidates;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private static final class Platform {
        private final String os;
        private final String arch;
        private final List<String> libraryNames;

        private Platform(String os, String arch, List<String> libraryNames) {
            this.os = os;
            this.arch = arch;
            this.libraryNames = libraryNames;
        }
    }
}
