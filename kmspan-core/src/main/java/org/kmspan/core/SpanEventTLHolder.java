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
    private static ThreadLocal<List<SpanMessage>> spanEvents = new ThreadLocal<List<SpanMessage>>() {
        @Override
        protected List<SpanMessage> initialValue() {
            return new ArrayList<>();
        }
    };

    public static SpanMessageHandler getSpanEventHandler() {
        return spanEventHandler.get();
    }

    public static void setSpanEventHandler(SpanMessageHandler handler) {
        spanEventHandler.set(handler);
    }

    public static List<SpanMessage> getSpanEvents() {
        return spanEvents.get();
    }
}
