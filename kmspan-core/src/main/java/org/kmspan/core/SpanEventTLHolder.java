package org.kmspan.core;

import java.util.ArrayList;
import java.util.List;

public class SpanEventTLHolder {
    private static ThreadLocal<SpanMessageHandler> spanEventHandler = new ThreadLocal<SpanMessageHandler>() {
        @Override
        protected SpanMessageHandler initialValue() {
            return null;
        }
    };
    private static ThreadLocal<List<ConsumerSpanEvent>> spanEvents = new ThreadLocal<List<ConsumerSpanEvent>>() {
        @Override
        protected List<ConsumerSpanEvent> initialValue() {
            return new ArrayList<>();
        }
    };

    public static SpanMessageHandler getSpanEventHandler() {
        return spanEventHandler.get();
    }

    public static void setSpanEventHandler(SpanMessageHandler handler) {
        spanEventHandler.set(handler);
    }

    public static List<ConsumerSpanEvent> getSpanEvents() {
        return spanEvents.get();
    }
}
