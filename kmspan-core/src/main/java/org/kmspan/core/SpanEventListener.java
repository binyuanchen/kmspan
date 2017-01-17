package org.kmspan.core;

// TODO create async ann
public interface SpanEventListener {
    void onSpanEvent(ConsumerSpanEvent consumerSpanEvent);
}
