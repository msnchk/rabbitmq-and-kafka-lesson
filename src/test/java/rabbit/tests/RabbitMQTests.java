package rabbit.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import rabbit.config.DefaultExchangeConfig;
import rabbit.config.ExchangeRoutingConfig;
import rabbit.data.LogEventDataProvider;
import rabbit.helpers.MessageHelper;
import rabbit.model.LogEvent;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQTests {
    private ConnectionFactory factory;
    private ObjectMapper mapper;

    @BeforeMethod
    public void setUp() {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        mapper = new ObjectMapper();
    }

    @Test
    public void shouldTestDefaultExchange() throws InterruptedException, IOException, TimeoutException {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            String queueName = DefaultExchangeConfig.QUEUE_NAME;
            channel.queueDeclare(queueName, false, false, true, null);

            LogEvent sentEvent = LogEvent.builder().build();
            String sentJson = mapper.writeValueAsString(sentEvent);
            channel.basicPublish("", queueName, null, sentJson.getBytes());

            LogEvent receivedEvent = MessageHelper.getResponse(channel, mapper, queueName);

            SoftAssert soft = new SoftAssert();
            soft.assertNotNull(receivedEvent, "Message was not received!");
            soft.assertEquals(receivedEvent.getLevel(), sentEvent.getLevel(), "Level mismatch");
            soft.assertEquals(receivedEvent.getMessage(), sentEvent.getMessage(), "Message mismatch");
            soft.assertAll();
        }
    }

    @Test(dataProvider = "logEventData", dataProviderClass = LogEventDataProvider.class)
    public void shouldTestExchangeWithRoutingKey(LogEvent sentEvent) throws InterruptedException, IOException, TimeoutException {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            String queueName = sentEvent.getQueueName();
            String exchangeName = ExchangeRoutingConfig.EXCHANGE_NAME;
            channel.queueDeclare(queueName, false, false, true, null);
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true);
            channel.queueBind(queueName, exchangeName, sentEvent.getLevel());

            String sentJson = mapper.writeValueAsString(sentEvent);
            channel.basicPublish(exchangeName, sentEvent.getLevel(), null, sentJson.getBytes());

            LogEvent receivedEvent = MessageHelper.getResponse(channel, mapper, queueName);

            SoftAssert soft = new SoftAssert();
            soft.assertNotNull(receivedEvent, "Message was not received!");
            soft.assertEquals(receivedEvent.getQueueName(), sentEvent.getQueueName(), "Queue mismatch");
            soft.assertEquals(receivedEvent.getLevel(), sentEvent.getLevel(), "Level mismatch");
            soft.assertEquals(receivedEvent.getMessage(), sentEvent.getMessage(), "Message mismatch");
            soft.assertAll();
        }
    }
}
