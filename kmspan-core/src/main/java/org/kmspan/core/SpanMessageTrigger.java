package org.kmspan.core;

/**
 * This interface is supposed to be implemented at producer side to generate span messages.
 */
public interface SpanMessageTrigger {
    /**
     * signals that a span begins
     */
    public void beginSpan(String topic, String spanId);

    /**
     * signals that a span ends
     */
    public void endSpan(String topic, String spanId);
}
