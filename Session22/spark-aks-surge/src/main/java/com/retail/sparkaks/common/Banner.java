package com.retail.sparkaks.common;

/**
 * Console formatting helper that enforces the training-delivery structure:
 *
 *   1. PROBLEM STATEMENT  (what breaks in production)
 *   2. REQUIREMENT        (what "operating safely" must guarantee)
 *   3. PROPOSED SOLUTION  (which Spark-on-AKS mechanism solves it)
 *   4. LIVE DEMO          (simulated behavior, step by step)
 *
 * Plain ASCII only, so output renders identically in IntelliJ's Run window
 * on macOS (Apple Silicon) and Windows cmd/PowerShell.
 */
public final class Banner {

    private Banner() {}

    private static final String LINE = "=".repeat(80);
    private static final String THIN = "-".repeat(80);

    public static void topic(int number, String title) {
        System.out.println();
        System.out.println(LINE);
        System.out.println("  TOPIC " + number + " :: " + title);
        System.out.println(LINE);
    }

    public static void problem(String... lines)     { section("1) PROBLEM STATEMENT", lines); }
    public static void requirement(String... lines) { section("2) REQUIREMENT", lines); }
    public static void solution(String... lines)    { section("3) PROPOSED SOLUTION (Spark on AKS)", lines); }
    public static void demo(String... lines)        { section("4) LIVE DEMO (simulation)", lines); }

    public static void step(String text) {
        System.out.println();
        System.out.println(">>> " + text);
    }

    public static void out(String text) { System.out.println("    " + text); }

    public static void warn(String text) { System.out.println("    !! " + text); }

    public static void keyTakeaway(String... lines) {
        System.out.println();
        System.out.println(THIN);
        System.out.println("  KEY TAKEAWAY");
        for (String l : lines) System.out.println("    * " + l);
        System.out.println(THIN);
    }

    private static void section(String header, String... lines) {
        System.out.println();
        System.out.println("[" + header + "]");
        for (String l : lines) System.out.println("    " + l);
    }

    public static void pause() {
        // Hook for instructors: uncomment to pause between topics during class.
        // new java.util.Scanner(System.in).nextLine();
    }
}
