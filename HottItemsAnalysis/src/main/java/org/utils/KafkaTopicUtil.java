package org.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaTopicUtil {

    /**
     * 建立 Kafka Topic（若已存在則忽略）
     *
     * @param bootstrapServers localhost:9092
     * @param topicName topic 名稱
     * @param partitions 分區數
     * @param replicationFactor 副本數
     */
    public static void createTopicIfNotExists(
            String bootstrapServers,
            String topicName,
            int partitions,
            short replicationFactor
    ) {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {

            NewTopic newTopic = new NewTopic(
                    topicName,
                    partitions,
                    replicationFactor
            );

            adminClient.createTopics(Collections.singletonList(newTopic))
                    .all()
                    .get();

            System.out.println("Kafka topic created: " + topicName);

        } catch (ExecutionException e) {
            // Topic 已存在時 Kafka 會丟這個例外，直接忽略即可
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                System.out.println("Kafka topic already exists: " + topicName);
            } else {
                throw new RuntimeException("Failed to create topic: " + topicName, e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }
    }
}
