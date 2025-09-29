package kafka.tests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.helpers.AssertionsHelper;
import kafka.helpers.KafkaHelper;
import kafka.helpers.MessagesGenerator;
import kafka.model.EventMessage;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaTests {
    private ObjectMapper mapper;
    private List<String> topicsToDelete = new ArrayList<>();

    @BeforeMethod
    public void setUp() {
        mapper = new ObjectMapper();
    }

    @Test
    public void oneTopicShouldStoreAndDeliverMessages() throws JsonProcessingException, ExecutionException, InterruptedException {
        String topicName = "transactions";
        int countOfMessages = 5;

        KafkaHelper.createTopics(List.of(topicName));
        topicsToDelete.add(topicName);
        try (KafkaProducer<String, String> producer = KafkaHelper.createKafkaProducer();
             KafkaConsumer<String, String> consumer = KafkaHelper.createKafkaConsumer("group")) {
            consumer.subscribe(List.of(topicName));

            Map<String, EventMessage> messages = MessagesGenerator.generateMessages(countOfMessages);
            KafkaHelper.sendMessages(messages, mapper, producer, topicName);
            producer.flush();
            Map<String, EventMessage> receivedMessages = KafkaHelper.getMessages(consumer, mapper);

            SoftAssert soft = new SoftAssert();
            AssertionsHelper.compareSentAndReceivedMessages(messages, receivedMessages, soft);
            soft.assertAll();
        }
    }

    @Test
    public void consumerShouldGetMessagesFromTwoTopics() throws ExecutionException, InterruptedException, JsonProcessingException {
        String ordersTopicName = "orders";
        String paymentsTopicName = "payments";
        int countOfMessages = 2;

        KafkaHelper.createTopics(List.of(ordersTopicName, paymentsTopicName));
        topicsToDelete.add(ordersTopicName);
        topicsToDelete.add(paymentsTopicName);
        try (KafkaProducer<String, String> producer = KafkaHelper.createKafkaProducer();
             KafkaConsumer<String, String> consumer = KafkaHelper.createKafkaConsumer("group")) {
            consumer.subscribe(List.of(ordersTopicName, paymentsTopicName));

            Map<String, EventMessage> messages = new HashMap<>();
            for(int i = 0; i < countOfMessages; i++) {
                EventMessage orderMessage = EventMessage.builder().build();
                messages.put(orderMessage.getKey(), orderMessage);
                KafkaHelper.sendMessage(orderMessage, mapper, producer, ordersTopicName);

                EventMessage paymentMessage = EventMessage.builder().build();
                messages.put(paymentMessage.getKey(), paymentMessage);
                KafkaHelper.sendMessage(paymentMessage, mapper, producer, paymentsTopicName);
            }
            producer.flush();
            Map<String, EventMessage> receivedMessages = KafkaHelper.getMessages(consumer, mapper);

            SoftAssert soft = new SoftAssert();
            AssertionsHelper.compareSentAndReceivedMessages(messages, receivedMessages, soft);
            soft.assertAll();
        }
    }

    @Test
    public void oneTopicShouldDeliverSameMessagesInTwoGroups() throws ExecutionException, InterruptedException, JsonProcessingException {
        String topicName = "user-actions";
        int countOfMessages = 2;

        KafkaHelper.createTopics(List.of(topicName));
        topicsToDelete.add(topicName);
        try (KafkaProducer<String, String> producer = KafkaHelper.createKafkaProducer();
             KafkaConsumer<String, String> consumerA = KafkaHelper.createKafkaConsumer("groupA");
             KafkaConsumer<String, String> consumerB = KafkaHelper.createKafkaConsumer("groupB")) {
            consumerA.subscribe(List.of(topicName));
            consumerB.subscribe(List.of(topicName));

            Map<String, EventMessage> messages = MessagesGenerator.generateMessages(countOfMessages);
            KafkaHelper.sendMessages(messages, mapper, producer, topicName);
            producer.flush();
            Map<String, EventMessage> receivedMessagesA = KafkaHelper.getMessages(consumerA, mapper);
            Map<String, EventMessage> receivedMessagesB = KafkaHelper.getMessages(consumerB, mapper);

            SoftAssert soft = new SoftAssert();
            AssertionsHelper.compareSentAndReceivedMessages(messages, receivedMessagesA, soft);
            AssertionsHelper.compareSentAndReceivedMessages(messages, receivedMessagesB, soft);
            soft.assertAll();
        }
    }

    @AfterClass
    private void tearDown() throws ExecutionException, InterruptedException {
        KafkaHelper.deleteTopics(topicsToDelete);
    }
}
