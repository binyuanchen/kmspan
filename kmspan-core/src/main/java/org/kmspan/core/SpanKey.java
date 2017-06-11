package org.kmspan.core;

/**
 * This is the wire (on Kafka) format of the key of a message in a span. It is either a span message
 * or a user message. Use this method {@link #isSpanMessage() isSpanMessage} to determine the nature
 * of this message.
 * <p>
 */
public class SpanKey<T> {
    // a global (the scope of messaging cluster) unique id for a single span
    private String spanId;
    // if not null, the type of a span event, in case this is null, it is an user message
    private String spanEventType;
    // the generic 'data', for Kafka, this is the actual message key that users want
    private T data;

    public SpanKey() {
        this(null, null, null);
    }

    public SpanKey(T data) {
        this(null, null, data);
    }

    public SpanKey(String spanId, String spanEventType, T data) {
        this.spanId = spanId;
        this.spanEventType = spanEventType;
        this.data = data;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getSpanEventType() {
        return spanEventType;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "SpanKey{" +
                "spanId='" + spanId + '\'' +
                ", spanEventType='" + spanEventType + '\'' +
                ", data=" + data +
                '}';
    }

    /**
     * This is the current way of determine a wire message is a span message or a user message.
     *
     * @return
     */
    public boolean isSpanMessage() {
        return getSpanEventType() != null;
    }
}
