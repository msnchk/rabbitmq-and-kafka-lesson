package kafka.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.config.KafkaConfig;
import kafka.model.EventMessage;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaHelper {
    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfig.AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        return new KafkaConsumer<>(props);
    }

    public static void sendMessages(Map<String, EventMessage> messages, ObjectMapper mapper, KafkaProducer<String, String> producer, String topicName) throws JsonProcessingException {
        for (Map.Entry<String, EventMessage> entry : messages.entrySet()) {
            String sentJson = mapper.writeValueAsString(entry.getValue());
            producer.send(new ProducerRecord<>(topicName, entry.getKey(), sentJson));
        }
    }

    public static void sendMessage(EventMessage message, ObjectMapper mapper, KafkaProducer<String, String> producer, String topicName) throws JsonProcessingException {
        String sentJson = mapper.writeValueAsString(message);
        producer.send(new ProducerRecord<>(topicName, message.getKey(), sentJson));
    }

    public static Map<String, EventMessage> getMessages(KafkaConsumer<String, String> consumer, ObjectMapper mapper) throws JsonProcessingException {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        Map<String, EventMessage> receivedMessages = new HashMap<>();
        for (ConsumerRecord<String, String> record: records) {
            EventMessage event = mapper.readValue(record.value(), EventMessage.class);
            receivedMessages.put(record.key(), event);
            }
        return receivedMessages;
    }

    public static void createTopics(List<String> topicNames) throws ExecutionException, InterruptedException {
        try (AdminClient admin = AdminClient.create(kafkaAdminProps())) {
            List<NewTopic> newTopics = new ArrayList<>();
            for (String name : topicNames) {
                newTopics.add(new NewTopic(name, KafkaConfig.TOPIC_PARTITIONS, KafkaConfig.TOPIC_REPLICATION_FACTOR));
            }
            CreateTopicsResult result = admin.createTopics(newTopics);
            result.all().get();
        }
    }

    public static void deleteTopics(List<String> topicNames) throws ExecutionException, InterruptedException {
        try (AdminClient admin = AdminClient.create(kafkaAdminProps())) {
            DeleteTopicsResult result = admin.deleteTopics(topicNames);
            result.all().get();
        }
    }

    private static Properties kafkaAdminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        return props;
    }
}
