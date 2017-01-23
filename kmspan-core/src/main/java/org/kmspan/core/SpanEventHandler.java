package org.kmspan.core;

import java.util.List;

/**
 * A handler that process span messages, collaborate with other handlers to generate span events.
 * Also allowing registeration of {@link SpanEventListener listeners} that want to be called back
 * when span events are generated.
 */
public interface SpanEventHandler {
    void handle(List<ConsumerSpanEvent> consumerSpanEvents);

    void registerSpanEventListener(SpanEventListener listener);
}
