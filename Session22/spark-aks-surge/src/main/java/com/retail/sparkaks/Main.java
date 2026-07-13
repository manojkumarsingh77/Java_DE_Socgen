package com.retail.sparkaks;

import com.retail.sparkaks.common.Banner;
import com.retail.sparkaks.pdb.PdbDemo;
import com.retail.sparkaks.resourcetuning.ResourceTuningDemo;
import com.retail.sparkaks.rollingupdate.RollingUpdateDemo;
import com.retail.sparkaks.sparkpods.DriverExecutorDemo;

/**
 * ============================================================================
 *  BLACK FRIDAY SURGE HANDLING
 *  Operating Apache Spark on AKS Safely - Instructor-Led Code Demo
 * ============================================================================
 *
 * How to run in IntelliJ (macOS Apple Silicon or Windows, Java 17):
 *   1. Open this folder as a Maven project (File -> Open -> select this dir).
 *   2. Let IntelliJ import the Maven model (no internet download needed -
 *      this project has ZERO third-party dependencies).
 *   3. Right-click Main.java -> Run 'Main.main()'.
 *   4. Optionally pass a single argument to run just one topic: 1, 2, 3, or 4.
 *
 * Each topic prints, in order: PROBLEM STATEMENT -> REQUIREMENT ->
 * PROPOSED SOLUTION -> LIVE DEMO -> KEY TAKEAWAY. Pair this console output
 * with DESIGN_NOTES.md, which maps every topic to the exact class/method
 * that drives its solution.
 */
public class Main {

    public static void main(String[] args) {
        System.out.println();
        System.out.println("############################################################################");
        System.out.println("#  BLACK FRIDAY SURGE HANDLING - Operating Spark on AKS Safely             #");
        System.out.println("#  Topics: Driver/Executor Pods | Resource Tuning | PDBs | Rolling Updates #");
        System.out.println("############################################################################");

        String only = args.length > 0 ? args[0].trim() : null;

        if (only == null || only.equals("1")) DriverExecutorDemo.run();
        if (only == null || only.equals("2")) ResourceTuningDemo.run();
        if (only == null || only.equals("3")) PdbDemo.run();
        if (only == null || only.equals("4")) RollingUpdateDemo.run();

        Banner.topic(0, "END OF DEMO - see DESIGN_NOTES.md for the class/method map per topic");
    }
}
