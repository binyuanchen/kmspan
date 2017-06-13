package org.kmspan.core;

import java.util.List;

/**
 * A SpanMessageHandler processes span messages, collaborate with other handlers to generate span events and notifies
 * {@link SpanEventListener event listeners} of generated span events.
 * <p>
 * The listener that is interested to be notified when span events are generate must register through the
 * registerListener api first.
 */
public interface SpanMessageHandler {
    void handle(List<ConsumerSpanEvent> consumerSpanEvents);

    void registerSpanEventListener(SpanEventListener listener);
}
