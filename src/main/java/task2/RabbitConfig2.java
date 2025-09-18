package task2;

public class RabbitConfig2 {
    public static final String EXCHANGE_NAME = "logs";
    public static final String ERROR_QUEUE = "error_logs";
    public static final String INFO_QUEUE = "info_logs";
    public static final String ERROR_ROUTING_KEY = "error";
    public static final String INFO_ROUTING_KEY = "info";
}
