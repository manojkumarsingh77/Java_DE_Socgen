import java.util.concurrent.CompletableFuture;

public class OrchestrationGraphDemo {

    public static void main(String[] args) {

        CompletableFuture<String> customer =
                CompletableFuture.supplyAsync(() -> fetchCustomer());

        CompletableFuture<String> transactions =
                customer.thenApply(c -> fetchTransactions(c));

        CompletableFuture<String> offers =
                customer.thenApply(c -> fetchOffers(c));

        CompletableFuture<String> finalResult =
                transactions.thenCombine(offers,
                        (txn, offer) -> txn + " + " + offer);

        System.out.println(finalResult.join());
    }

    static String fetchCustomer() {
        sleep(1000);
        return "Customer123";
    }

    static String fetchTransactions(String customer) {
        sleep(1000);
        return "Transactions";
    }

    static String fetchOffers(String customer) {
        sleep(800);
        return "Offers";
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}