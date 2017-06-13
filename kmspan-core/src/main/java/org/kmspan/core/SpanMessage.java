package org.kmspan.core;

import org.apache.kafka.common.record.TimestampType;

/**
 * A span message. See related {@link SpanEvent span event} for the distinction between
 * a span message and a span event.
 */
public class SpanMessage {
    //the timestamp type of the raw Kafka message
    private TimestampType kafkaTimestampType = null;
    //the timestamp of the raw Kafka message
    private long kafkaTimestamp = -1;

    private String spanId;
    private String spanEventType;
    private String topic;

    private SpanMessage(TimestampType kafkaTimestampType, long kafkaTimestamp,
                        String spanId, String spanEventType, String topic) {
        this.kafkaTimestampType = kafkaTimestampType;
        this.kafkaTimestamp = kafkaTimestamp;
        this.spanId = spanId;
        this.spanEventType = spanEventType;
        this.topic = topic;
    }

    // a util method
    public static SpanMessage createSpanMessage(TimestampType kafkaTimestampType,
                                                long kafkaTimestamp,
                                                String spanId,
                                                String spanEventType,
                                                String topic) {
        return new SpanMessage(kafkaTimestampType, kafkaTimestamp, spanId, spanEventType, topic);
    }

    public static SpanMessage createSpanEvent(String spanId,
                                              String spanEventType,
                                              String topic) {
        return new SpanMessage(null, -1, spanId, spanEventType, topic);
    }

    public TimestampType getKafkaTimestampType() {
        return kafkaTimestampType;
    }

    public long getKafkaTimestamp() {
        return kafkaTimestamp;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getSpanEventType() {
        return spanEventType;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "SpanMessage{" +
                "kafkaTimestampType=" + kafkaTimestampType +
                ", kafkaTimestamp=" + kafkaTimestamp +
                ", spanId='" + spanId + '\'' +
                ", spanEventType='" + spanEventType + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
