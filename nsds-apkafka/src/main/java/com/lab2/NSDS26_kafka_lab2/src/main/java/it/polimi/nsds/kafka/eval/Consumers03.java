package com.lab2.NSDS26_kafka_lab2.src.main.java.it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;

import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.Duration;
import java.util.*;

// Group number:
// Group members:

// Is it possible to have more than one partition for topics "sensors1" and "sensors2"?

// Is there any relation between the number of partitions in "sensors1" and "sensors2"?

// Is it possible to have more than one instance of Merger?
// If so, what is the relation between their group id?

// Is it possible to have more than one partition for topic "merged"?

// Is it possible to have more than one instance of Validator?
// If so, what is the relation between their group id?

public class Consumers03 {

    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];

        // TODO: add arguments if necessary
        switch (stage) {
            case 0:
                new Merger(serverAddr, groupId).execute();
                break;
            case 1:
                new Validator(serverAddr, groupId).execute();
                break;
            case 2:
                System.err.println("Wrong stage");
        }
    }

    private static class Merger {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String topicSensors1 = "sensors1";
        private static final String topicSensors2 = "sensors2";
        private static final String topicMerger   = "merged";

        // TODO: add attributes if necessary
        private boolean autoCommit = false;

        // TODO: add arguments if necessary
        public Merger(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));

            // TODO: add properties if needed

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Arrays.asList(topicSensors1, topicSensors2));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

            // TODO: add properties if needed

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            // Hashmaps to keep the latest values for each key
            HashMap<String, Integer> latestSensors1 = new HashMap<>();
            HashMap<String, Integer> latestSensors2 = new HashMap<>();

            // TODO: implement the processing logic
            while (true) {
                // TODO: implement the processing logic
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for(final ConsumerRecord<String, Integer> record : records) {
                    String key = record.key();

                    if(record.topic().equals(topicSensors1)) {
                        latestSensors1.put(key, record.value());
                    } else if (record.topic().equals(topicSensors2)) {
                        latestSensors2.put(key, record.value());
                    }

                    int sum = latestSensors1.getOrDefault(key, 0) + latestSensors2.getOrDefault(key, 0);

                    // **  Prints to debug in stdout
                    System.out.println("Merger | " + record.topic() + " key=" + key + " value=" + record.value()
                            + " | s1=" + latestSensors1.getOrDefault(key, 0)
                            + " s2=" + latestSensors2.getOrDefault(key, 0) + " -> merged=" + sum);

                    producer.send(new ProducerRecord<>(topicMerger, key, sum));
                }

                producer.flush();

                consumer.commitSync();
            }
        }
    }

    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;
        private static final String outputSensors1  = "output1";
        private static final String outputSensors2  = "output2";
        private static final String topicMerger     = "merged";
        private static final String transactionalId = "valTransactionalId";
        private static boolean autoCommit = false;
        
        // TODO: add attributes if needed

        public Validator(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // TODO: add properties if needed

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(topicMerger));
            
            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            // TODO: add properties if needed

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();
            // TODO: implement the processing logic
            while (true) {
                // TODO: implement the processing logic
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for(final ConsumerRecord<String, Integer> record : records) {
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(outputSensors1, record.key(), record.value()));
                    producer.send(new ProducerRecord<>(outputSensors2, record.key(), record.value()));

                    // Prints to debug in stdout
                    System.out.println("Validator | key=" + record.key() + " value=" + record.value()
                            + " -> output1, output2 (offset " + record.offset() + ")");

                    // The producer manually commits the offsets for the consumer within the transaction so that every transaction is commited along with an offset
                    // meaning at most 1 message will get resent minimizing the number of duplicated messages
                    final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();
                }
            }
        }
    }
}