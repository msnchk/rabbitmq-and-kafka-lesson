package rabbit.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import rabbit.model.LogEvent;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class MessageHelper {
    public static LogEvent getResponse (Channel channel, ObjectMapper mapper, String queueName) throws InterruptedException, IOException {
        BlockingQueue<LogEvent> response = new ArrayBlockingQueue<>(1);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            LogEvent received = mapper.readValue(delivery.getBody(), LogEvent.class);
            response.add(received);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
        LogEvent receivedEvent = response.poll(5, TimeUnit.SECONDS);
        return receivedEvent;
    }
}
