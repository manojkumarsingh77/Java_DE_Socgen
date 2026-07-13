import java.util.HashMap;
import java.util.Map;

public class PaymentServiceWithIdempotency {

    private final Map<String, Double> accounts = new HashMap<>();
    private final Map<String, String> processedRequests = new HashMap<>();

    public PaymentServiceWithIdempotency() {
        accounts.put("Rahul", 100000.0);
    }

    public String debit(String requestId,
                        String customer,
                        double amount) {

        if (processedRequests.containsKey(requestId)) {
            return "Duplicate request ignored. Previous result: "
                    + processedRequests.get(requestId);
        }

        double balance = accounts.get(customer);

        accounts.put(customer, balance - amount);

        String result =
                "Debited " + amount +
                        ", Balance: " + accounts.get(customer);

        processedRequests.put(requestId, result);

        return result;
    }

    public static void main(String[] args) {

        PaymentServiceWithIdempotency service =
                new PaymentServiceWithIdempotency();

        String requestId = "PAY-123";

        System.out.println(service.debit(requestId, "Rahul", 10000));
        System.out.println(service.debit(requestId, "Rahul", 10000));
    }
}