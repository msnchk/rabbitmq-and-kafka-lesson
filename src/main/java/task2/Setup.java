package task2;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Setup {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(RabbitConfig2.ERROR_QUEUE, true, false, false, null);
            channel.queueDeclare(RabbitConfig2.INFO_QUEUE, true, false, false, null);
            channel.exchangeDeclare(RabbitConfig2.EXCHANGE_NAME, BuiltinExchangeType.DIRECT, true);

            channel.queueBind(RabbitConfig2.ERROR_QUEUE, RabbitConfig2.EXCHANGE_NAME, RabbitConfig2.ERROR_ROUTING_KEY);
            channel.queueBind(RabbitConfig2.INFO_QUEUE, RabbitConfig2.EXCHANGE_NAME, RabbitConfig2.INFO_ROUTING_KEY);
        }
    }
}
