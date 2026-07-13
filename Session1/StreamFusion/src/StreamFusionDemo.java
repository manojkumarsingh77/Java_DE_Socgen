import java.util.List;

public class StreamFusionDemo {

    public static void main(String[] args) {

        List<Transaction> transactions = List.of(
                new Transaction("T1", "C101", 10000, "INDIA", 20),
                new Transaction("T2", "C102", 90000, "USA", 90),
                new Transaction("T3", "C103", 75000, "UK", 85),
                new Transaction("T4", "C104", 30000, "INDIA", 10)
        );

        List<FraudCandidate> fraudList = transactions.stream()
                .filter(txn -> {
                    System.out.println("Filter Amount: " + txn.txnId());
                    return txn.amount() > 50000;
                })
                .map(txn -> {
                    System.out.println("Mapping: " + txn.txnId());
                    return new FraudCandidate(
                            txn.txnId(),
                            txn.customerId(),
                            txn.amount()
                    );
                })
                .filter(candidate -> {
                    System.out.println("Final Filter: " + candidate.txnId());
                    return candidate.amount() > 70000;
                })
                .toList();

        System.out.println("\nFraud Candidates:");
        fraudList.forEach(System.out::println);
    }
}