package org.kmspan.core;

public class SpanConstants {
    public final static String SPAN_BEGIN = "SPAN_BEGIN";
    public final static String SPAN_END = "SPAN_END";

    public final static String SPAN_BEGIN_SC_ZPATH = "span.begin.zpath";
    public final static String DEFAULT_SPAN_BEGIN_SC_ZPATH = "/kmspan/sc/begin";

    public final static String SPAN_END_SC_ZPATH = "span.end.zpath";
    public final static String DEFAULT_SPAN_END_SC_ZPATH = "/kmspan/sc/end";

    public final static String SPAN_ZK_QUORUM = "span.zookeeper.quorum";
    public final static String DEFAULT_SPAN_ZK_QUORUM = "localhost:2181";
}
