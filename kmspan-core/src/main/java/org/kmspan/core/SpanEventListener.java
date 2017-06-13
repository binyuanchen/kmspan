package org.kmspan.core;

/**
 * Users can create listeners and register them with {@link SpanMessageHandler SpanMessageHandler}
 * to be called back when span events are generated.
 */
public interface SpanEventListener {
    // TODO create async ann
    void onSpanEvent(ConsumerSpanEvent consumerSpanEvent);
}
