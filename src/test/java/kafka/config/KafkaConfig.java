package kafka.config;

public class KafkaConfig {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String AUTO_OFFSET_RESET = "earliest";
    public static final int TOPIC_PARTITIONS = 1;
    public static final short TOPIC_REPLICATION_FACTOR = 1;
}
