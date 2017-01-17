package org.kmspan.camel;

import org.kmspan.core.serialization.SpanDataSerDeser;

/**
 * how user materialize typed {@link SpanDataSerDeser SpanDataSerDeser<T>}, in
 * this case, user would like to send/receive message with key of type String.
 */
public class TestUserSpanDataStringSerDser extends SpanDataSerDeser<String> {
}
