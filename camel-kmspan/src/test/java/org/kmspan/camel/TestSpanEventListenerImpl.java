package org.kmspan.camel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.SpanEvent;
import org.kmspan.core.SpanEventListener;

/**
 * how user handle received span event, here user just logs.
 */
public class TestSpanEventListenerImpl implements SpanEventListener {
    private static Logger LOG = LogManager.getLogger(TestSpanEventListenerImpl.class);

    @Override
    public void onSpanEvent(SpanEvent event) {
        LOG.info("[span] event spanId={}, spanEventType={}",
                event.getSpanId(), event.getSpanType());
    }
}
