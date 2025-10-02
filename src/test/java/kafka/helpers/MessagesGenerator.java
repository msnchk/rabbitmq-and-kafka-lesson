package kafka.helpers;

import kafka.model.EventMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class MessagesGenerator {
    public static Map<String, EventMessage> generateMessages(int count) {
        Map<String, EventMessage> messages = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String key = Integer.toString(new Random().nextInt(1000));
            messages.put(key, new EventMessage("some message text" + i, key));
        }
        return messages;
    }
}
