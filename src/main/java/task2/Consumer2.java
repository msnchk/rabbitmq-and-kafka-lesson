package task2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Consumer2 {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        DeliverCallback errorCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(addTime() + " NEW ERROR MESSAGE: '" + message + "'");
        };
        DeliverCallback infoCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(addTime() + " NEW INFO MESSAGE: '" + message + "'");
        };

        channel.basicConsume(RabbitConfig2.ERROR_QUEUE, true, errorCallback, consumerTag -> { });
        channel.basicConsume(RabbitConfig2.INFO_QUEUE, true, infoCallback, consumerTag -> { });
    }

    public static String addTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
