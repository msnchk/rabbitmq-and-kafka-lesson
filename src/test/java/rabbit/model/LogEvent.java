package rabbit.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LogEvent {
    @Builder.Default
    private String level = "unknown";

    @Builder.Default
    private String message = "default message text";

    @Builder.Default
    private String queueName = "";
}
