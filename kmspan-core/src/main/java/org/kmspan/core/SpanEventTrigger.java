package org.kmspan.core;

public interface SpanEventTrigger {
    /**
     * signals that a span begins
     */
    public void beginSpan(String topic, String spanId);

    /**
     * signals that a span ends
     */
    public void endSpan(String topic, String spanId);
}
