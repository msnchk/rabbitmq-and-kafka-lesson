package kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Random;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EventMessage {
    @Builder.Default
    private String text = "default message text";

    @Builder.Default
    private String key = Integer.toString(new Random().nextInt(1000));
}
