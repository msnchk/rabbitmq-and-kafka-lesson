package kafka.helpers;

import kafka.model.EventMessage;
import org.testng.asserts.SoftAssert;

import java.util.Map;

public class AssertionsHelper {
    public static void compareSentAndReceivedMessages(
            Map<String, EventMessage> sentMessages,
            Map<String, EventMessage> receivedMessages,
            SoftAssert soft) {
        soft.assertEquals(sentMessages.size(), receivedMessages.size());
        for (String key : sentMessages.keySet()) {
            EventMessage sent = sentMessages.get(key);
            EventMessage received = receivedMessages.get(key);
            soft.assertNotNull(received, "Message with key " + key + " was not received");
            soft.assertEquals(sent.getText(), received.getText(), "Message " + key + ": text field mismatch");
            soft.assertEquals(sent.getKey(), received.getKey(), "Message " + key + ": key field mismatch");
        }
    }
}
