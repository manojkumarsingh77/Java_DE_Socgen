public class ImmutableDemo {

    public static void main(String[] args) {

        ImmutableTransactionDTO txn =
                new ImmutableTransactionDTO("TXN1001", "CUST101", 10000);

        System.out.println("Amount: " + txn.getAmount());

        // accidental modification
        txn.setAmount(0); // compile error

        System.out.println("Modified Amount: " + txn.getAmount());
    }
}