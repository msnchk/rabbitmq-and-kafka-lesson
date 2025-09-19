package rabbit.data;

import rabbit.config.ExchangeRoutingConfig;
import rabbit.model.LogEvent;
import org.testng.annotations.DataProvider;

public class LogEventDataProvider {
    @DataProvider(name = "logEventData")
    public Object[][] logEventData() {
        return new Object[][]{
                { new LogEvent(
                        ExchangeRoutingConfig.ERROR_ROUTING_KEY,
                        "Some error message",
                        ExchangeRoutingConfig.ERROR_QUEUE) },

                { new LogEvent(
                        ExchangeRoutingConfig.INFO_ROUTING_KEY,
                        "Some info message",
                        ExchangeRoutingConfig.INFO_QUEUE
                ) },
        };
    }
}
