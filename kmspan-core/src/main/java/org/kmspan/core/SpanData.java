package org.kmspan.core;

/**
 * This is the wire (for example, on Fafka) format of the key of a message in a span. This can be a
 * span event, or a user message. When {@link #spanEventType spanEventType} is {@code null}, this is
 * a user message, otherwise, this is a span event.
 * <p>
 * For Kafka, this
 * format applies to the
 */
public class SpanData<T> {
    // a global (the scope of messaging cluster) unique id for a single span
    private String spanId;
    // if not null, the type of a span event, in case this is null, it is an user message
    private String spanEventType;
    // the time in milliseconds when this message/event is generated
    private long generationTimestamp;
    // the generic 'data', for Kafka, this is the actual message key that users want
    private T data;

    public SpanData() {
        this.generationTimestamp = System.currentTimeMillis();
    }

    public SpanData(long generationTimestamp, T data) {
        this(null, null, generationTimestamp, data);
    }

    public SpanData(String spanId, String spanEventType, long generationTimestamp, T data) {
        this.spanId = spanId;
        this.spanEventType = spanEventType;
        this.generationTimestamp = generationTimestamp;
        this.data = data;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getSpanEventType() {
        return spanEventType;
    }

    public long getGenerationTimestamp() {
        return generationTimestamp;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "SpanData{" +
                "spanId='" + spanId + '\'' +
                ", spanEventType='" + spanEventType + '\'' +
                ", generationTimestamp=" + generationTimestamp +
                ", data=" + data +
                '}';
    }

    public boolean isSpanMessage() {
        return getSpanEventType() != null;
    }
}
