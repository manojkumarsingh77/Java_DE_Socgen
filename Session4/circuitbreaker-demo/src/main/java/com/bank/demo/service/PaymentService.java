package com.bank.demo.service;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class PaymentService {

    private final Random random = new Random();

    @CircuitBreaker(
            name = "paymentGateway",
            fallbackMethod = "fallbackPayment"
    )
    public String processPayment() {

        int chance = random.nextInt(10);

        if (chance < 7) {
            throw new RuntimeException("External Payment Gateway Timeout");
        }

        return "Payment processed successfully";
    }

    public String fallbackPayment(Exception ex) {
        return "Payment gateway unavailable. Please try after some time.";
    }
}