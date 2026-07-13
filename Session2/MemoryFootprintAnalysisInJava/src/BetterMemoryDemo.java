public class BetterMemoryDemo {

    public static void main(String[] args) {

        Runtime runtime = Runtime.getRuntime();

        printMemory("Before processing", runtime);

        for (int i = 0; i < 100; i++) {
            processChunk();
        }

        System.gc();

        printMemory("After processing", runtime);
    }

    private static void processChunk() {
        byte[] temp = new byte[1024 * 1024];
    }

    private static void printMemory(String label, Runtime runtime) {
        long used = runtime.totalMemory() - runtime.freeMemory();

        System.out.println(label);
        System.out.println("Used Memory MB: " + used / (1024 * 1024));
    }
}