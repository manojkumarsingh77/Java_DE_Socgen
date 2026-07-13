package com.fintechpay;

import io.github.resilience4j.circuitbreaker.*;

import java.time.Duration;

public class BankingDemo {

    public static void main(String[] args)
            throws Exception {

        CircuitBreakerConfig config =
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .minimumNumberOfCalls(5)
                        .slidingWindowSize(10)
                        .waitDurationInOpenState(
                                Duration.ofSeconds(5))
                        .permittedNumberOfCallsInHalfOpenState(3)
                        .build();

        CircuitBreaker cb =
                CircuitBreaker.of(
                        "paymentGateway",
                        config);

        // State Transition Listener

        cb.getEventPublisher()
                .onStateTransition(event ->
                        System.out.println(
                                "\n***************\n" +
                                        "STATE CHANGE : "
                                        + event.getStateTransition()
                                        + "\n***************\n"));

        PaymentGateway gateway =
                new PaymentGateway();

        for(int i = 1; i <= 30; i++) {

            System.out.println(
                    "\nRequest #" + i);

            try {

                String result =
                        CircuitBreaker
                                .decorateSupplier(
                                        cb,
                                        gateway::processPayment)
                                .get();

                System.out.println(
                        "SUCCESS : "
                                + result);

            }
            catch(Exception ex) {

                System.out.println(
                        "FAILED : "
                                + ex.getMessage());
            }

            System.out.println(
                    "Current State = "
                            + cb.getState());

            Thread.sleep(1000);
        }
    }
}