package com.bank.gcdemo;

public class AuthorizationService {

    private final FraudScoringEngine fraudScoringEngine =
            new FraudScoringEngine();

    public boolean authorize(PaymentRequest request) {

        boolean fraudPassed = fraudScoringEngine.score(request);

        String response =
                "TXN-" + request.getTxnId()
                        + "-APPROVED-" + System.nanoTime();

        return fraudPassed && response.length() > 0;
    }
}