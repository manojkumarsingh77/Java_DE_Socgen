public class MutableTransactionDTO {

    private String transactionId;
    private String customerId;
    private double amount;

    public MutableTransactionDTO(String transactionId, String customerId, double amount) {
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

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }
}