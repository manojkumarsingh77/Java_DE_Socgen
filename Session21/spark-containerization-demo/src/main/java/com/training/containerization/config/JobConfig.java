package com.training.containerization.config;

/**
 * Every value here can be overridden by an environment variable of the same name.
 * <p>
 * WHY THIS CLASS EXISTS (training note):
 * In the "Resource Constraints" and "JVM Tuning in Containers" sections of the demo,
 * we do NOT want learners editing Java code and rebuilding an image every time they
 * want to try a different memory/CPU budget. Instead, every knob is externalised as
 * an env var, so the same immutable image can be re-run with different
 * docker-compose.yml / `docker run -e` values. This is exactly how production data
 * platforms are tuned per-environment (dev/stage/prod) without rebuilding images.
 */
public class JobConfig {

    public final String demoMode;
    public final String sparkMaster;
    public final int shufflePartitions;
    public final int recordCount;
    public final String driverMemory;

    public final int memStressStepMb;
    public final double memStressStopPercent;

    public final int cpuStressThreads;
    public final int cpuStressDurationSec;

    private JobConfig(String demoMode, String sparkMaster, int shufflePartitions, int recordCount,
                       String driverMemory, int memStressStepMb, double memStressStopPercent,
                       int cpuStressThreads, int cpuStressDurationSec) {
        this.demoMode = demoMode;
        this.sparkMaster = sparkMaster;
        this.shufflePartitions = shufflePartitions;
        this.recordCount = recordCount;
        this.driverMemory = driverMemory;
        this.memStressStepMb = memStressStepMb;
        this.memStressStopPercent = memStressStopPercent;
        this.cpuStressThreads = cpuStressThreads;
        this.cpuStressDurationSec = cpuStressDurationSec;
    }

    public static JobConfig fromEnvAndArgs(String[] args) {
        String mode = args != null && args.length > 0 ? args[0] : env("DEMO_MODE", "all");
        return new JobConfig(
                mode,
                env("SPARK_MASTER", "local[*]"),
                envInt("SHUFFLE_PARTITIONS", 4),
                envInt("RECORD_COUNT", 50_000),
                env("SPARK_DRIVER_MEMORY", "1g"),
                envInt("MEM_STRESS_STEP_MB", 25),
                envDouble("MEM_STRESS_STOP_PERCENT", 90.0),
                envInt("CPU_STRESS_THREADS", Runtime.getRuntime().availableProcessors()),
                envInt("CPU_STRESS_DURATION_SEC", 8)
        );
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    private static int envInt(String key, int def) {
        try {
            return Integer.parseInt(env(key, String.valueOf(def)));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static double envDouble(String key, double def) {
        try {
            return Double.parseDouble(env(key, String.valueOf(def)));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    @Override
    public String toString() {
        return "JobConfig{" +
                "demoMode='" + demoMode + '\'' +
                ", sparkMaster='" + sparkMaster + '\'' +
                ", shufflePartitions=" + shufflePartitions +
                ", recordCount=" + recordCount +
                ", driverMemory='" + driverMemory + '\'' +
                ", memStressStepMb=" + memStressStepMb +
                ", memStressStopPercent=" + memStressStopPercent +
                ", cpuStressThreads=" + cpuStressThreads +
                ", cpuStressDurationSec=" + cpuStressDurationSec +
                '}';
    }
}
