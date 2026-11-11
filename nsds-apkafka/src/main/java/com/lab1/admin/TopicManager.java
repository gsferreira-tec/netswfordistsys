package com.lab1.admin;

import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class TopicManager  {
    private static final String defaultTopicName = "topicA";
    private static final int defaultTopicPartitions = 5; // exercise 2 change # of paritions from 2 to 5 kill one of AnotherBasicConsumer of the multiples running and check that the others will receive the partitions from the other killed one
    private static final short defaultReplicationFactor = 1;

    private static final String serverAddr = "localhost:9092";

    public static void main(String[] args) throws Exception {
        final String topicName = args.length >= 1 ? args[0] : defaultTopicName;
        final int topicPartitions = args.length >= 2 ? Integer.parseInt(args[1]) : defaultTopicPartitions;
        final short replicationFactor = args.length >= 3 ? Short.parseShort(args[2]) : defaultReplicationFactor;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        AdminClient adminClient = AdminClient.create(props);

        ListTopicsResult listResult = adminClient.listTopics();
        Set<String> topicsNames = listResult.names().get();
        System.out.println("Available topics: " + topicsNames);

        if (topicsNames.contains(topicName)) {
            System.out.println("Deleting topic " + topicName);
            DeleteTopicsResult delResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            delResult.all().get();
            System.out.println("Done!");
            // Wait for the deletion
            Thread.sleep(5000);
        }

        System.out.println("Adding topic " + topicName + " with " + topicPartitions + " partitions");
        NewTopic newTopic = new NewTopic(topicName, topicPartitions, replicationFactor);
        CreateTopicsResult createResult = adminClient.createTopics(Collections.singletonList(newTopic));
        createResult.all().get();
        System.out.println("Done!");
    }
}