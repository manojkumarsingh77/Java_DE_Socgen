import java.util.concurrent.CompletableFuture;

public class CompletableFutureBankingDemo {

    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        CompletableFuture<String> kycFuture =
                CompletableFuture.supplyAsync(() -> checkKyc());

        CompletableFuture<String> fraudFuture =
                CompletableFuture.supplyAsync(() -> checkFraud());

        CompletableFuture<String> balanceFuture =
                CompletableFuture.supplyAsync(() -> checkBalance());

        CompletableFuture<String> finalDecision =
                kycFuture.thenCombine(fraudFuture,
                                (kyc, fraud) -> kyc + " | " + fraud)
                        .thenCombine(balanceFuture,
                                (partial, balance) -> partial + " | " + balance);

        System.out.println("Decision: " + finalDecision.join());

        long end = System.currentTimeMillis();

        System.out.println("Total Time: " + (end - start) + " ms");
    }

    static String checkKyc() {
        sleep(1000);
        return "KYC OK";
    }

    static String checkFraud() {
        sleep(1500);
        return "FRAUD CLEAR";
    }

    static String checkBalance() {
        sleep(1200);
        return "BALANCE OK";
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}