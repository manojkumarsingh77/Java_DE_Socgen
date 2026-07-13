package com.bank.retail.streaming.app1;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * ============================================================================
 *  APP 1 - KAFKA TOPIC SETUP
 * ============================================================================
 * BUSINESS PROBLEM THIS SOLVES:
 * Before any retail-banking order can flow through the pipeline, the Kafka
 * topics it depends on must exist with the right partition count. Running
 * this once (idempotently - safe to re-run) guarantees Apps 2-5 always have
 * a topic to talk to, instead of failing with "UnknownTopicOrPartition".
 *
 * >>> THE METHOD THAT SOLVES THIS PROBLEM IS: createTopicIfMissing() <<<
 *
 * You can run this app from IntelliJ, OR achieve the same result with the
 * docker CLI commands documented in README.md - both are shown so you can
 * see the equivalence between "do it with code" and "do it with the CLI".
 *
 * NO special VM options are required to run this class (plain Kafka client,
 * no Spark involved).
 * ============================================================================
 */
public class KafkaTopicSetupApp {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // The two topics this demo uses:
    //  - retail.payments.orders : raw customer payment orders (App2 -> App3)
    //  - retail.payments.dlq    : a dead-letter style topic for orders App3
    //                             could not even parse (not used by the
    //                             happy-path demo, but created so you can
    //                             extend the demo with malformed-message
    //                             handling without an extra setup step).
    private static final String ORDERS_TOPIC = "retail.payments.orders";
    private static final String DLQ_TOPIC = "retail.payments.dlq";

    public static void main(String[] args) throws Exception {

        // AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG: tells the admin client
        // which broker(s) to contact first to discover the rest of the
        // cluster's metadata. Your docker-compose Kafka container exposes
        // 9092 on localhost per the `docker ps` output you shared.
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // try-with-resources: AdminClient holds a network connection: this
        // guarantees it is closed even if topic creation throws.
        try (AdminClient adminClient = AdminClient.create(props)) {

            System.out.println("Connecting to Kafka at " + BOOTSTRAP_SERVERS + " ...");

            Set<String> existingTopics = adminClient.listTopics().names().get();
            System.out.println("Existing topics: " + existingTopics);

            createTopicIfMissing(adminClient, existingTopics, ORDERS_TOPIC, 3, (short) 1);
            createTopicIfMissing(adminClient, existingTopics, DLQ_TOPIC, 1, (short) 1);

            // Re-list to prove it worked - this is the verification step a
            // real setup script always includes rather than assuming success.
            List<String> finalTopics = adminClient.listTopics().listings().get()
                    .stream().map(TopicListing::name).collect(Collectors.toList());
            System.out.println("Topics now present on the broker: " + finalTopics);
        }
    }

    /**
     * >>> SOLUTION METHOD <<<
     * Creates a Kafka topic only if it doesn't already exist - this is what
     * makes the whole app idempotent (safe to run over and over, e.g. every
     * time a student starts the demo).
     *
     * @param partitions      how many partitions to split the topic into.
     *                        More partitions = more parallelism for Spark's
     *                        consumers later, at the cost of weaker per-key
     *                        ordering guarantees across partitions.
     * @param replicationFactor how many broker replicas hold each partition.
     *                        We use 1 because the docker-compose setup you
     *                        shared has a SINGLE Kafka broker; in production
     *                        this would typically be 3.
     */
    private static void createTopicIfMissing(AdminClient adminClient,
                                               Set<String> existingTopics,
                                               String topicName,
                                               int partitions,
                                               short replicationFactor)
            throws ExecutionException, InterruptedException {

        if (existingTopics.contains(topicName)) {
            System.out.println("Topic '" + topicName + "' already exists - skipping.");
            return;
        }

        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
        adminClient.createTopics(List.of(newTopic)).all().get();
        System.out.println("Created topic '" + topicName + "' with "
                + partitions + " partitions, replication factor " + replicationFactor);
    }
}
