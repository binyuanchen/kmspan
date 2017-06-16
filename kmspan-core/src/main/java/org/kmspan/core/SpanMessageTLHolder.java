package org.kmspan.core;

import java.util.ArrayList;
import java.util.List;

public class SpanMessageTLHolder {
    private static ThreadLocal<SpanMessageHandler> spanMessageHandler = new ThreadLocal<SpanMessageHandler>() {
        @Override
        protected SpanMessageHandler initialValue() {
            return null;
        }
    };
    private static ThreadLocal<List<SpanMessage>> spanMessages = new ThreadLocal<List<SpanMessage>>() {
        @Override
        protected List<SpanMessage> initialValue() {
            return new ArrayList<>();
        }
    };

    public static SpanMessageHandler getSpanMessageHandler() {
        return spanMessageHandler.get();
    }

    public static void setSpanMessageHandler(SpanMessageHandler handler) {
        spanMessageHandler.set(handler);
    }

    public static List<SpanMessage> getSpanMessages() {
        return spanMessages.get();
    }

    public static void cleanupBoth() {
        spanMessageHandler.remove();

    }
}