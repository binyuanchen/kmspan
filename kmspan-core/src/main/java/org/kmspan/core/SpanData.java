package org.kmspan.core;

public class SpanData<T> {
    private String spanId;
    private String spanEventType;
    private long generationTimestamp;
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
}
