package org.kmspan.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SampleSpanEventListener implements SpanEventListener {
    private static Logger logger = LogManager.getLogger(SampleSpanEventListener.class);

    /**
     * This method is called for each span event. In the unit test, this method is spied.
     *
     * @param event
     */
    @Override
    public void onSpanEvent(SpanEvent event) {
        // here we simply log the event, but you may choose to, for example, raise an
        // event to UI, or persist the event into a datastore etc.
        logger.info("[span]consumer received span event: {}", event);
    }
}
