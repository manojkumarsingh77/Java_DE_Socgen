import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class G1GCBankingDemo {

    static class PaymentRequest {
        private final String customerId;
        private final byte[] payload;

        PaymentRequest(String customerId, int payloadSizeKB) {
            this.customerId = customerId;
            this.payload = new byte[payloadSizeKB * 1024];
        }
    }

    public static void main(String[] args) throws InterruptedException {

        Random random = new Random();

        List<PaymentRequest> longLivedCache = new ArrayList<>();

        for (int cycle = 1; cycle <= 1000; cycle++) {

            // Short-lived payment traffic
            for (int i = 0; i < 5000; i++) {
                PaymentRequest request =
                        new PaymentRequest("CUST-" + i, 10);

                processPayment(request);
            }

            // Simulate some long-lived cached objects
            if (cycle % 50 == 0) {
                for (int i = 0; i < 100; i++) {
                    longLivedCache.add(
                            new PaymentRequest(
                                    "VIP-" + random.nextInt(1000),
                                    50
                            )
                    );
                }
            }

            System.out.println(
                    "Cycle " + cycle +
                            " | Long-lived cache size: " +
                            longLivedCache.size()
            );

            Thread.sleep(100);
        }
    }

    private static void processPayment(PaymentRequest request) {
        String txnId = "TXN-" + System.nanoTime();
        String audit = request.customerId + "-" + txnId;

        if (audit.hashCode() == 999999) {
            System.out.println("Impossible");
        }
    }
}