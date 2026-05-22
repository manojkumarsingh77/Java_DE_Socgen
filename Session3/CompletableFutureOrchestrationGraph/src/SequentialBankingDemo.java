public class SequentialBankingDemo {

    public static void main(String[] args) {

        long start = System.currentTimeMillis();

        String kyc = checkKyc();
        String fraud = checkFraud();
        String balance = checkBalance();

        System.out.println("Decision: " + kyc + " | " + fraud + " | " + balance);

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