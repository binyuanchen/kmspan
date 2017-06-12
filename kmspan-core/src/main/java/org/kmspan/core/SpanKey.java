package org.kmspan.core;

/**
 * This is the wired format of the key of a message in a span that flows through the messaging framework (for example,
 * Kafka). It is either a span message or a user message. Use method {@link #isSpan() isSpan} to differentiate.
 * <p>
 */
public class SpanKey<T> {
    // a global (in the scope of messaging cluster) unique id for a single span
    private String id;
    // if not null, the type of a span message, in case this is null, it is an user message
    private String type;
    // the user message key
    private T data;

    public SpanKey() {
        this(null, null, null);
    }

    public SpanKey(T data) {
        this(null, null, data);
    }

    public SpanKey(String spanId, String type, T data) {
        this.id = spanId;
        this.type = type;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
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
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", data=" + data +
                '}';
    }

    /**
     * @return true, if this message is a span message, false, if this message is a user message
     */
    public boolean isSpan() {
        return getType() != null;
    }
}
