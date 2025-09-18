package task2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Producer2 {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            String errorMessage = "Some error message";
            channel.basicPublish(RabbitConfig2.EXCHANGE_NAME, RabbitConfig2.ERROR_ROUTING_KEY, null, errorMessage.getBytes());
            System.out.println(" [x] Sent '" + errorMessage + "'");

            String infoMessage = "Some info message";
            channel.basicPublish(RabbitConfig2.EXCHANGE_NAME, RabbitConfig2.INFO_ROUTING_KEY, null, infoMessage.getBytes());
            System.out.println(" [x] Sent '" + infoMessage + "'");
        }
    }
}
