package org.kmspan.camel;

public final class KmspanConstants {

    // mode or just default (only choice for now)
    public static final String MODE_OR_DEFAULT = "default";

    // source types
    public static final String SOURCE_TYPE_KAFKA = "kafka";

    // storage types
    public static final String STORAGE_TYPE_ZOOKEEPER = "zookeeper";

    //Span event listener registry key
    public static final String SPAN_EVENT_LISTENER_REGISTRY_NAME = "span.event.listener";

    private KmspanConstants() {
    }
}
