public record FraudCandidate(
        String txnId,
        String customerId,
        double amount
) {
}