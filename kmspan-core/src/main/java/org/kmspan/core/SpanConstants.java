package org.kmspan.core;

public class SpanConstants {
    /**
     * This identifies a message as the begin event of a span, see {@link SpanData#spanEventType}
     */
    public final static String SPAN_BEGIN = "SPAN_BEGIN";

    /**
     * This identifies a message as the end event of a span, see {@link SpanData#spanEventType}
     */
    public final static String SPAN_END = "SPAN_END";

    /**
     * For span to generate span boundary events, it relies on zookeeper, particularly zookeeper
     * recipe shared count. Each span uses two different shared counts, one for begin event, the
     * other for end event.
     * <p>
     * This is the key that kmspan users use to configure the shared count common path prefix
     * used for span begin event.
     */
    public final static String SPAN_BEGIN_SC_ZPATH = "span.begin.zpath";

    /**
     * the default shared count common path prefix for span begin events
     */
    public final static String DEFAULT_SPAN_BEGIN_SC_ZPATH = "/kmspan/sc/begin";

    /**
     * This is the key that kmspan users use to configure the shared count common path prefix
     * used for span end event.
     */
    public final static String SPAN_END_SC_ZPATH = "span.end.zpath";

    /**
     * the default shared count common path prefix for span end events
     */
    public final static String DEFAULT_SPAN_END_SC_ZPATH = "/kmspan/sc/end";

    /**
     * This is the key that kmspan users use to specify a zookeeper cluster quorum location
     */
    public final static String SPAN_ZK_QUORUM = "span.zookeeper.quorum";

    /**
     * This is the zookeeper cluster quorum location, for example:
     * {@code "server1:2181,server2:2181,server3:2181"}
     */
    public final static String DEFAULT_SPAN_ZK_QUORUM = "localhost:2181";

    /**
     * refer to {@link org.kmspan.core.SpanProcessingStrategy.Mode mode} for this
     */
    public final static String SPAN_PROCESSING_MODE = "span.processing.mode";

    public final static String DEFAULT_SPAN_PROCESSING_MODE = "rough";
}
