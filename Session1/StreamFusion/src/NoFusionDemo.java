import java.util.ArrayList;
import java.util.List;

public class NoFusionDemo {

    public static void main(String[] args) {

        List<Transaction> transactions = List.of(
                new Transaction("T1", "C101", 10000, "INDIA", 20),
                new Transaction("T2", "C102", 90000, "USA", 90),
                new Transaction("T3", "C103", 75000, "UK", 85)
        );

        List<Transaction> filtered = new ArrayList<>();

        for (Transaction txn : transactions) {
            if (txn.amount() > 50000) {
                filtered.add(txn);
            }
        }

        List<FraudCandidate> mapped = new ArrayList<>();

        for (Transaction txn : filtered) {
            mapped.add(new FraudCandidate(
                    txn.txnId(),
                    txn.customerId(),
                    txn.amount()
            ));
        }

        List<FraudCandidate> finalList = new ArrayList<>();

        for (FraudCandidate fc : mapped) {
            if (fc.amount() > 70000) {
                finalList.add(fc);
            }
        }

        System.out.println(finalList);
    }
}