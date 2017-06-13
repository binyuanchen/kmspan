package org.kmspan.core;

/**
 * A span event. Do not confuse this with {@link SpanMessage span message}. A span message is sent out to
 * each Kafka topic partition, eventually is handled by one {@link SpanMessageHandler handler}. Handlers
 * collaborate through Zookeeper cluster, eventually one handler generates a span event. A span event is
 * supposed to be delivered to {@link SpanEventListener span event listener}.
 * <p>
 * All span messages for the a particular span event share something in common:
 * <p>
 * 1. they share the same span id
 * 2. they share the same span type
 * 3. they come from the same Kafka topic
 */
public class SpanEvent {
    private String spanId;
    private String spanType;
    private String topic;

    /**
     * The local timestamp assigned by the {@link SpanMessageHandler handler} that generates this event
     */
    private long localGenerationTime = -1;

    public SpanEvent(long localGenerationTime, String spanId, String spanType, String topic) {
        this.localGenerationTime = localGenerationTime;
        this.spanId = spanId;
        this.spanType = spanType;
        this.topic = topic;
    }

    public long getLocalGenerationTime() {
        return localGenerationTime;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getSpanType() {
        return spanType;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "SpanEvent{" +
                "spanId='" + spanId + '\'' +
                ", spanType='" + spanType + '\'' +
                ", topic='" + topic + '\'' +
                ", localGenerationTime=" + localGenerationTime +
                '}';
    }
}
