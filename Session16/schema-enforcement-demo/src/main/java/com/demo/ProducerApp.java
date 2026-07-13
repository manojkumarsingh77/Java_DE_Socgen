package com.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class ProducerApp {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(props);

        String goodRecord =
                """
                {
                  "transaction_id":"TX1001",
                  "customer_id":"C101",
                  "amount":2500,
                  "transaction_type":"DEBIT"
                }
                """;

        String badRecord =
                """
                {
                  "txn_id":"TX1002",
                  "customer_id":"C102",
                  "amount":"ABC",
                  "transaction_type":"DEBIT"
                }
                """;

        RecordMetadata goodMetadata =
                producer.send(
                                new ProducerRecord<>(
                                        "banking-transactions",
                                        goodRecord))
                        .get();

        System.out.println(
                "GOOD RECORD SENT -> Partition="
                        + goodMetadata.partition()
                        + " Offset="
                        + goodMetadata.offset());

        RecordMetadata badMetadata =
                producer.send(
                                new ProducerRecord<>(
                                        "banking-transactions",
                                        badRecord))
                        .get();

        System.out.println(
                "BAD RECORD SENT -> Partition="
                        + badMetadata.partition()
                        + " Offset="
                        + badMetadata.offset());

        producer.flush();
        producer.close();

        System.out.println("Producer Completed Successfully");
    }
}