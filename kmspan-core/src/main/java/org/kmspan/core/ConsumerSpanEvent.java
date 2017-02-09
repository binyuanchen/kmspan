package org.kmspan.core;

import org.apache.kafka.common.record.TimestampType;

/**
 * This is used by Kafka consumer side to capture essential information of a span message,
 * or a span event. Whether it is a span message of a span event depending on the context
 * where it is being used.
 */
public class ConsumerSpanEvent {
    // when used as a span message, this is the timestamp type of the wire Kafka message,
    // when used as a span event, this is null
    private TimestampType kafkaTimestampType = null;
    private long kafkaTimestamp = -1;

    private String spanId;
    private String spanEventType;
    private String topic;

    private ConsumerSpanEvent(TimestampType kafkaTimestampType, long kafkaTimestamp,
                              String spanId, String spanEventType, String topic) {
        this.kafkaTimestampType = kafkaTimestampType;
        this.kafkaTimestamp = kafkaTimestamp;
        this.spanId = spanId;
        this.spanEventType = spanEventType;
        this.topic = topic;
    }

    public static ConsumerSpanEvent createSpanMessage(TimestampType kafkaTimestampType,
                                                      long kafkaTimestamp,
                                                      String spanId,
                                                      String spanEventType,
                                                      String topic) {
        return new ConsumerSpanEvent(kafkaTimestampType, kafkaTimestamp, spanId, spanEventType, topic);
    }

    public static ConsumerSpanEvent createSpanEvent(String spanId,
                                                    String spanEventType,
                                                    String topic) {
        return new ConsumerSpanEvent(null, -1, spanId, spanEventType, topic);
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

    public boolean isSpanEvent() {
        return this.kafkaTimestampType == null;
    }

    @Override
    public String toString() {
        return "ConsumerSpanEvent{" +
                "kafkaTimestampType=" + kafkaTimestampType +
                ", kafkaTimestamp=" + kafkaTimestamp +
                ", spanId='" + spanId + '\'' +
                ", spanEventType='" + spanEventType + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
