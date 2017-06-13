package org.kmspan.camel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.ConsumerSpanEvent;
import org.kmspan.core.SpanEventListener;

/**
 * how user handle received span event, here user just logs.
 */
public class TestSpanEventListenerImpl implements SpanEventListener {
    private static Logger logger = LogManager.getLogger(TestSpanEventListenerImpl.class);

    @Override
    public void onSpanEvent(ConsumerSpanEvent consumerSpanEvent) {
        logger.info("[span] event spanId={}, spanEventType={}",
                consumerSpanEvent.getSpanId(),
                consumerSpanEvent.getSpanEventType());
    }
}
