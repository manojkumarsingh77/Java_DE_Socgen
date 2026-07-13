package com.training.containerization;

import com.training.containerization.config.JobConfig;
import com.training.containerization.diagnostics.ContainerDiagnostics;
import com.training.containerization.diagnostics.ResourceConstraintSimulator;
import com.training.containerization.job.SalesAnalyticsJob;
import org.apache.spark.sql.SparkSession;

/**
 * Single entry point for every demo mode used across this training module.
 * <p>
 * Usage:  java -jar app.jar [diagnostics | job | stress-memory | stress-cpu | all]
 * <p>
 * The mode is also readable from the DEMO_MODE environment variable, which is how
 * docker-compose.yml / docker run -e switch behaviour without rebuilding the image -
 * see docker/docker-compose.yml and DEMO-GUIDE.md.
 */
public class App {

    public static void main(String[] args) {
        JobConfig config = JobConfig.fromEnvAndArgs(args);

        System.out.println("############################################################");
        System.out.println("# Containerization for Data Platforms - Training Demo");
        System.out.println("# mode = " + config.demoMode);
        System.out.println("# " + config);
        System.out.println("############################################################");

        switch (config.demoMode) {
            case "diagnostics" -> ContainerDiagnostics.printReport();
            case "stress-memory" -> {
                ContainerDiagnostics.printReport();
                ResourceConstraintSimulator.simulateMemoryPressure(config);
            }
            case "stress-cpu" -> {
                ContainerDiagnostics.printReport();
                ResourceConstraintSimulator.simulateCpuPressure(config);
            }
            case "job" -> runSparkJob(config);
            case "all" -> {
                ContainerDiagnostics.printReport();
                runSparkJob(config);
                ResourceConstraintSimulator.simulateMemoryPressure(config);
                ResourceConstraintSimulator.simulateCpuPressure(config);
                System.out.println("\n[App] All demo modes complete. Re-run individual modes via the first");
                System.out.println("[App] argument, e.g.: java -jar app.jar diagnostics");
            }
            default -> {
                System.err.println("Unknown mode '" + config.demoMode + "'. Valid modes: "
                        + "diagnostics | job | stress-memory | stress-cpu | all");
                System.exit(1);
            }
        }
    }

    private static void runSparkJob(JobConfig config) {
        SparkSession spark = SalesAnalyticsJob.createSparkSession(config);
        try {
            new SalesAnalyticsJob().run(spark, config);
        } finally {
            spark.stop();
        }
    }
}
