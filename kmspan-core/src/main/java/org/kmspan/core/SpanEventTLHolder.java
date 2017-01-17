package org.kmspan.core;

import java.util.ArrayList;
import java.util.List;

public class SpanEventTLHolder {
    private static ThreadLocal<SpanEventHandler> spanEventHandler = new ThreadLocal<SpanEventHandler>() {
        @Override
        protected SpanEventHandler initialValue() {
            return null;
        }
    };
    private static ThreadLocal<List<ConsumerSpanEvent>> spanEvents = new ThreadLocal<List<ConsumerSpanEvent>>() {
        @Override
        protected List<ConsumerSpanEvent> initialValue() {
            return new ArrayList<>();
        }
    };

    public static SpanEventHandler getSpanEventHandler() {
        return spanEventHandler.get();
    }

    public static void setSpanEventHandler(SpanEventHandler handler) {
        spanEventHandler.set(handler);
    }

    public static List<ConsumerSpanEvent> getSpanEvents() {
        return spanEvents.get();
    }
}
