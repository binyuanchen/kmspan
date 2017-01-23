package org.kmspan.core;

/**
 * A Kafka consumer side representation of a span message, or a span event, depending used under what context.
 * For the difference between these two, please see <a href="https://github.com/binyuanchen/kmspan/wiki">kmspan wiki</a>
 */
public class ConsumerSpanEvent {
    private String spanId;
    private String spanEventType;
    private long generationTime;
    private String topic;

    public ConsumerSpanEvent(String spanId, String spanEventType, long generationTime, String topic) {
        this.spanId = spanId;
        this.spanEventType = spanEventType;
        this.generationTime = generationTime;
        this.topic = topic;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getSpanEventType() {
        return spanEventType;
    }

    public long getGenerationTime() {
        return generationTime;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "ConsumerSpanEvent{" +
                "spanId='" + spanId + '\'' +
                ", spanEventType='" + spanEventType + '\'' +
                ", generationTime=" + generationTime +
                ", topic='" + topic + '\'' +
                '}';
    }
}
