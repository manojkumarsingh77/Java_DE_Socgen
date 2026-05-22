import java.util.ArrayList;
import java.util.List;

public class BankingMemoryDemo {

    public static void main(String[] args) {

        Runtime runtime = Runtime.getRuntime();

        printMemory("Start", runtime);

        List<Transaction> transactions = new ArrayList<>();

        for (int i = 0; i < 1_000_000; i++) {
            transactions.add(
                    new Transaction(
                            "TXN" + i,
                            "CUST" + i,
                            "AMAZON",
                            1000
                    )
            );
        }

        printMemory("After creating 1 million transactions", runtime);
    }

    static void printMemory(String label, Runtime runtime) {
        long used = runtime.totalMemory() - runtime.freeMemory();

        System.out.println(label + ": " + used / (1024 * 1024) + " MB");
    }
}