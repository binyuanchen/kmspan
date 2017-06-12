package org.kmspan.core;

public class SpanConstants {
    /**
     * A span message type indicating the beginning of a span of messages. (Alternatively, this is called the 'left'
     * boundary of a span.)
     */
    public final static String SPAN_BEGIN = "SPAN_BEGIN";
    /**
     * A span message type indicating the ending of a span of messages. (Alternatively, this is called the 'right'
     * boundary of a span.)
     */
    public final static String SPAN_END = "SPAN_END";

    /**
     * For span to generate span boundary events, it relies on Zookeeper constructs, such as a shared count.
     * Each span utilizes two shared counts, one for span begin event (span left boundary), the other for span end event
     * (span right boundary).
     * <p>
     * The users of the kmspan framework must configure the common Zookeeper node path prefix where all the shared
     * counts for all span begin events live under. The key name of such path is 'span.begin.zpath', the default value
     * of this key is '/kmspan/sc/begin'.
     */
    public final static String SPAN_BEGIN_SC_ZPATH = "span.begin.zpath";
    public final static String DEFAULT_SPAN_BEGIN_SC_ZPATH = "/kmspan/sc/begin";

    /**
     * Similarly, the users of the kmspan framework must configure the common Zookeeper node path prefix where all the
     * shared counts for all span end events live under. The key name of such path is 'span.end.zpath', the default value
     * of this key is '/kmspan/sc/end'.
     */
    public final static String SPAN_END_SC_ZPATH = "span.end.zpath";
    public final static String DEFAULT_SPAN_END_SC_ZPATH = "/kmspan/sc/end";

    /**
     * The users of the kmspan framework must configure a zookeeper cluster quorum where all Zookeeper constructs that
     * the kmspan framework uses will be stored into. The key name of this quorum is 'span.zookeeper.quorum', the default
     * value of this key is 'localhost:2181'.
     */
    public final static String SPAN_ZK_QUORUM = "span.zookeeper.quorum";
    /**
     * For example:
     * {@code "server1:2181,server2:2181,server3:2181"}
     */
    public final static String DEFAULT_SPAN_ZK_QUORUM = "localhost:2181";

    /**
     * Please refer to {@link org.kmspan.core.SpanProcessingStrategy.Mode mode} for the reasoning
     */
    public final static String SPAN_PROCESSING_MODE = "span.processing.mode";
    public final static String DEFAULT_SPAN_PROCESSING_MODE = "rough";
}
