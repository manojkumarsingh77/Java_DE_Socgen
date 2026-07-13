package com.demo.streaming;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaProducerSimulator {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer =
                new KafkaProducer<>(props);

        while(true){

            for(int i=0;i<50000;i++){

                ProducerRecord<String,String> record =
                        new ProducerRecord<>(
                                "telecom-events",
                                "Tower Failure Event " + i);

                producer.send(record);
            }

            System.out.println("50K messages sent");

        }

    }
}