package org.kmspan.core;


public class SpanProcessingStrategy {

    /**
     * The {@link #ROUGH rough} mode and {@link #PRECISE precise} mode are mutual exclusive.
     */
    public enum Mode {
        /**
         * Rough mode means:
         * <p>
         * 1. user can not use the {@link SpanKafkaConsumer#pollWithSpan(long) pollWithSpan} api.
         * 2. user use the {@link SpanKafkaConsumer#poll(long) poll} api, and get back
         * {@link org.apache.kafka.clients.consumer.ConsumerRecords ConsumerRecords}, but user also must
         * pass the ConsumerRecords to a method annotated with {@link org.kmspan.core.annotation.Spaned Spaned}.
         * 3. internally, the {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long) raw poll} api is
         * used to poll messages off the Kafka broker, which may contain only user messages or a mix of span messages
         * and user messages. For these messages, in rough mode:
         * <p>
         * 3.1 user messages are not sorted by {@link SpanData#generationTimestamp generation timestamp},
         * <p>
         * 3.2 the span BEGIN messages are sorted by {@link SpanData#generationTimestamp generation timestamp}, and are
         * processed before the execution of the {@link org.kmspan.core.annotation.Spaned Spaned} annotated method,
         * the span END message are sorted also {@link SpanData#generationTimestamp generation timestamp}, and are
         * processed after the execution of the {@link org.kmspan.core.annotation.Spaned Spaned} annotated method.
         * <p>
         * Futher details on the 'rough' mode can be found out at github wiki
         * <a href="https://github.com/binyuanchen/kmspan/wiki/Internals-of-kmspan">kmspan wiki</a>
         */
        ROUGH("rough") // default

        /**
         * precise mode means:
         * <p>
         * 1. user uses the {@link SpanKafkaConsumer#pollWithSpan(long) pollWithSpan} api, and get back a
         * {@link org.kmspan.core.SpanKafkaConsumer.SpanIterable OrderedMixedIterable}, then user follows the regular way
         * of iterating over this OrderedMixedIterable.
         * 2. user can not use {@link SpanKafkaConsumer#poll(long) poll} api.
         * 3. internally, the {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long) raw poll} api is
         * used to poll messages off the Kafka broker, which may contain only user messages or a mix of span messages
         * and user messages. For these messages, in precise mode: span messages (if any) and user messages (if any)
         * will be sorted together by {@link SpanData#generationTimestamp generation timestamp}, the order is then
         * honored by the resulting {@link org.kmspan.core.SpanKafkaConsumer.SpanIterable OrderedMixedIterable}.
         * <p>
         * Futher details on the 'precise' mode can be found out at github wiki
         * <a href="https://github.com/binyuanchen/kmspan/wiki/Internals-of-kmspan">kmspan wiki</a>
         */
        , PRECISE("precise")

        /**
         *
         */
        ;

        private final String name;

        Mode(String name) {
            this.name = name;
        }

        public static Mode getByName(String name) {
            for (Mode m : values()) {
                if (m.getName().equals(name)) {
                    return m;
                }
            }
            return null;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * this is not used yet
     */
    public static enum ProcessingMode {

        /**
         * all span messages are processed before poll() results are returned
         */
        PRIOR_TO_POLL_RETURN

        /**
         * all span BEGIN messages are processed
         */
        , BEGIN_SPANED_THEN_END

        /**
         * this implies {@link SpanMessageOrdering#ORDERED SpanMessageOrdering.ORDERED}
         */
        , PRECISE

        //
        ;
    }

    /**
     * this is not used yet
     */
    public static enum SpanMessageOrdering {
        /**
         * If {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long) poll} returns span messages,
         * they will be processed in order of {@link SpanData#generationTimestamp generation time}.
         */
        ORDERED

        /**
         *
         */
        , NOT_ORDERED // default

        //
        ;
    }

    /**
     * this is not used yet
     */
    public static enum UserMessageOrdering {
        /**
         * The user messages returned by {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long) poll} will
         * be ordered {@link SpanData#generationTimestamp generation time} before being further returned to client
         */
        ORDERED

        /**
         *
         */
        , NOT_ORDERED // default

        //
        ;
    }

}
