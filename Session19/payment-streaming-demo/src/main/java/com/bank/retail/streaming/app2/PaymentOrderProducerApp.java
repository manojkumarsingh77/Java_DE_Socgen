package com.bank.retail.streaming.app2;

import com.bank.retail.streaming.model.PaymentOrderEvent;
import com.bank.retail.streaming.util.JsonUtil;
import com.bank.retail.streaming.util.SyntheticDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ============================================================================
 *  APP 2 - PAYMENT ORDER PRODUCER
 * ============================================================================
 * BUSINESS PROBLEM THIS SOLVES:
 * In a real retail bank, every customer order (UPI/NEFT/IMPS/CARD payment)
 * becomes an event the instant it's placed, published onto a Kafka topic so
 * every downstream system (fraud engine, ledger, notifications, analytics)
 * can react to it independently. This app SIMULATES that "order placed"
 * moment for many customers, continuously, the same way a real
 * order-management service would.
 *
 * >>> THE METHOD THAT SOLVES THIS PROBLEM IS: publishOrders() <<<
 *
 * NO special VM options are required (plain Kafka client, no Spark).
 * Run this AFTER App1 has created the topic.
 * ============================================================================
 */
public class PaymentOrderProducerApp {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ORDERS_TOPIC = "retail.payments.orders";

    // How many synthetic orders to publish this run, and the random delay
    // band (ms) between sends, to mimic a real, slightly bursty traffic
    // pattern rather than one unrealistic instant burst.
    private static final int ORDERS_TO_SEND = 200;
    private static final int MIN_DELAY_MS = 50;
    private static final int MAX_DELAY_MS = 300;

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        // Which broker(s) to connect to.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Kafka messages are raw bytes on the wire; serializers convert our
        // Java String key/value into those bytes.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks=all : the broker only confirms the write after ALL in-sync
        // replicas have it. In a single-broker dev cluster this is the same
        // as acks=1, but it's the setting you'd actually use in production,
        // so the demo models real-world configuration, not a shortcut.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // enable.idempotence=true : guarantees that even if the producer has
        // to retry a send (e.g. transient network blip), Kafka will not
        // create a duplicate message. This is the standard way to get
        // "exactly once per producer session" semantics on the write side -
        // important for a PAYMENTS pipeline where double-processing an order
        // is a real financial bug, not just a logging nuisance.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        SyntheticDataGenerator generator = new SyntheticDataGenerator();

        // try-with-resources closes the producer (flushing any buffered
        // messages) automatically, even if publishOrders() throws.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            publishOrders(producer, generator);
        }
    }

    /**
     * >>> SOLUTION METHOD <<<
     * Generates ORDERS_TO_SEND synthetic retail-banking payment orders and
     * publishes each one as a JSON message to the orders topic.
     */
    private static void publishOrders(KafkaProducer<String, String> producer,
                                        SyntheticDataGenerator generator) throws InterruptedException {

        System.out.println("Publishing " + ORDERS_TO_SEND + " synthetic payment orders to topic '"
                + ORDERS_TOPIC + "' ...");

        for (int i = 1; i <= ORDERS_TO_SEND; i++) {

            PaymentOrderEvent order = generator.generateOrder();
            String json = JsonUtil.toJson(order);

            // We use the correlationId as the Kafka MESSAGE KEY (not just a
            // field inside the JSON value). WHY this matters: Kafka
            // guarantees all messages with the SAME key always land on the
            // SAME partition and are read back in the order they were sent.
            // For a payments system, that means if you ever published
            // multiple events for one transaction (e.g. "created" then
            // "updated"), they would still process in order. Here it also
            // gives Spark good parallelism: different correlationIds spread
            // evenly across the topic's 3 partitions.
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(ORDERS_TOPIC, order.getCorrelationId(), json);

            // send() is asynchronous - it returns immediately. The callback
            // fires later (on an internal producer I/O thread) once Kafka
            // has actually acknowledged the write (or failed it). We log
            // the partition/offset Kafka assigned, which is exactly the
            // kind of detail an SRE looks for first when chasing a "did
            // this message even get published?" question.
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                if (exception != null) {
                    System.err.println("FAILED to publish order " + order.getOrderId()
                            + " correlationId=" + order.getCorrelationId() + " : " + exception.getMessage());
                } else {
                    System.out.println(String.format(
                            "[%d/%d] sent orderId=%s correlationId=%s -> partition=%d offset=%d amount=%.2f channel=%s",
                            i, ORDERS_TO_SEND, order.getOrderId(), order.getCorrelationId(),
                            metadata.partition(), metadata.offset(), order.getAmount(), order.getChannel()));
                }
            });

            // Small randomized pause between sends so the demo produces a
            // believable, gradually-arriving stream for Spark to consume -
            // rather than one giant instantaneous batch.
            Thread.sleep(ThreadLocalRandom.current().nextInt(MIN_DELAY_MS, MAX_DELAY_MS));
        }

        // flush() blocks until every buffered/in-flight send has either
        // completed or failed - guarantees we don't exit the JVM (closing
        // the producer) while messages are still in transit.
        producer.flush();
        System.out.println("Finished publishing " + ORDERS_TO_SEND + " orders.");
    }
}
