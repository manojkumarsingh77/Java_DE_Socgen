import java.util.ArrayList;
import java.util.List;

public class BadMemoryDemo {

    public static void main(String[] args) {

        List<byte[]> allocations = new ArrayList<>();

        Runtime runtime = Runtime.getRuntime();

        printMemory("Before allocation", runtime);

        for (int i = 0; i < 100; i++) {
            allocations.add(new byte[1024 * 1024]); // 1 MB each
        }

        printMemory("After allocation", runtime);
    }

    private static void printMemory(String label, Runtime runtime) {
        long used = runtime.totalMemory() - runtime.freeMemory();

        System.out.println(label);
        System.out.println("Used Memory MB: " + used / (1024 * 1024));
    }
}