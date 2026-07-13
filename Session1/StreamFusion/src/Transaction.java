public record Transaction(
        String txnId,
        String customerId,
        double amount,
        String country,
        int riskScore
) {
}