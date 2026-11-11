package com.lab1.basic;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BasicConsumer {
    private static final String defaultGroupId = "groupA";
    private static final String defaultTopic = "topicA";

    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;

    // Default is "latest": try "earliest" instead
    private static final String offsetResetStrategy = "latest"; // latest starts from the last offset read from any consumer for that topic; earliest always starts from 0 receiving all messages

    public static void main(String[] args) {
        // If there are arguments, use the first as group and the second as topic.
        // Otherwise, use default group and topic.
        String groupId = args.length >= 1 ? args[0] : defaultGroupId;
        String topic = args.length >= 2 ? args[1] : defaultTopic;
        String newTopic = "topicB";

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // unique id for consumer group
        
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit)); // auto commit offsets of msgs
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // Exercise 3

        // from tutorial
        // KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props); // instatiating kafka consumer object - props needed are the lines above 29-37
        // consumer.subscribe(Collections.singletonList(topic)); // subscribe to a topic
        
        // from lab 1 
        KafkaConsumer<String, String> C1 = new KafkaConsumer<>(props); // instantiating consumer C1
        // C1.subscribe(Collections.singletonList(topic)); // consumer 1 subscribes to topicA
        C1.subscribe(Arrays.asList(topic, newTopic));


        while(true) {
            final ConsumerRecords<String, String> records = C1.poll(Duration.of(5, ChronoUnit.MINUTES)); // C1 is polls producer for records or times out if not available
            for (final ConsumerRecord<String, String> record : records) {
                System.out.print("Consumer group: " + groupId + "\t");
                System.out.println("Partition: " + record.partition() +
                        "\tOffset: " + record.offset() +
                        "\tKey: " + record.key() +
                        "\tValue: " + record.value() + 
                        "\tTopic: " + record.topic()
                ); // Consumer C1 prints the records to stdout
            }
        }
    }
}