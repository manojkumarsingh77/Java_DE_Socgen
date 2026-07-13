public record Transaction(
        String txnId,
        String customerId,
        String merchant,
        double amount
) {
}