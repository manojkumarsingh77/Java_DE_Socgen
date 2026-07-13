import java.util.HashMap;
import java.util.Map;

public class PaymentServiceWithoutIdempotency {

    private final Map<String, Double> accounts = new HashMap<>();

    public PaymentServiceWithoutIdempotency() {
        accounts.put("Rahul", 100000.0);
    }

    public void debit(String customer, double amount) {

        double balance = accounts.get(customer);

        accounts.put(customer, balance - amount);

        System.out.println("Debited " + amount);
        System.out.println("Balance: " + accounts.get(customer));
    }

    public static void main(String[] args) {

        PaymentServiceWithoutIdempotency service =
                new PaymentServiceWithoutIdempotency();

        service.debit("Rahul", 10000);
        service.debit("Rahul", 10000); // duplicate retry
    }
}