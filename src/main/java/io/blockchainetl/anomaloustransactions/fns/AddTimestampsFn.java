package io.blockchainetl.anomaloustransactions.fns;

import io.blockchainetl.anomaloustransactions.utils.JsonUtils;
import org.codehaus.jackson.JsonNode;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class AddTimestampsFn extends ErrorHandlingDoFn<String, String> {

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        String msg = c.element();
        Instant timestamp = getTimestampFromMessage(msg);
        c.outputWithTimestamp(msg, timestamp);
    }

    private static Instant getTimestampFromMessage(String msg) {
        JsonNode node = JsonUtils.parseJson(msg);
        JsonNode timestamp = node.get("timestamp");
        JsonNode blockTimestamp = node.get("block_timestamp");
        if (timestamp != null) {
            return new Instant(timestamp.asLong() * 1000);
        } else if (blockTimestamp != null) {
            return new Instant(blockTimestamp.asLong() * 1000);
        } else {
            throw new RuntimeException("There are no timestamp or block_timestamp fields in the message " + msg);
        }
    }

    @Override
    public Duration getAllowedTimestampSkew() {
        return Duration.standardHours(12);
    }
}
