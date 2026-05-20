public final class ImmutableTransactionDTO {

    private final String transactionId;
    private final String customerId;
    private final double amount;

    public ImmutableTransactionDTO(String transactionId,
                                   String customerId,
                                   double amount) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.amount = amount;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public double getAmount() {
        return amount;
    }
}