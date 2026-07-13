import java.util.concurrent.ConcurrentHashMap;

public class BetterGoldenCode {

    private final ConcurrentHashMap<String, Double> accounts =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, String> processed =
            new ConcurrentHashMap<>();

    public BetterGoldenCode() {
        accounts.put("Rahul", 100000.0);
    }

    public String debit(String requestId,
                        String customer,
                        double amount) {

        String existing = processed.get(requestId);

        if (existing != null) {
            return existing;
        }

        synchronized (customer.intern()) {

            existing = processed.get(requestId);
            if (existing != null) {
                return existing;
            }

            double balance = accounts.get(customer);

            if (balance < amount) {
                return "Insufficient funds";
            }

            accounts.put(customer, balance - amount);

            String result =
                    "Debited " + amount +
                            ", Balance: " + accounts.get(customer);

            processed.put(requestId, result);

            return result;
        }
    }
}