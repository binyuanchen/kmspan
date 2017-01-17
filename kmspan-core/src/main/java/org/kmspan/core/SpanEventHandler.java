package org.kmspan.core;

import java.util.List;

public interface SpanEventHandler {
    void handle(List<ConsumerSpanEvent> consumerSpanEvents);
    void registerSpanEventListener(SpanEventListener listener);
}
