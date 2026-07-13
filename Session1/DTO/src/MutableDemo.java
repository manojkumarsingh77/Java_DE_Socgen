public class MutableDemo {

    public static void main(String[] args) {

        MutableTransactionDTO txn =
                new MutableTransactionDTO("TXN1001", "CUST101", 10000);

        System.out.println("Original Amount: " + txn.getAmount());

        // accidental modification
        txn.setAmount(0);

        System.out.println("Modified Amount: " + txn.getAmount());
    }
}