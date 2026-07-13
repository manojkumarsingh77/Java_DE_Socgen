package com.fintechpay;

import java.util.Random;

public class PaymentGateway {

    private final Random random = new Random();

    public String processPayment() {

        int value = random.nextInt(100);

        // 70% failure rate

        if(value < 70) {

            System.out.println("Gateway Failure");

            throw new RuntimeException(
                    "Payment Service Down");
        }

        return "Payment Successful";
    }
}