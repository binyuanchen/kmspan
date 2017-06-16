package org.kmspan.core;


public class SpanProcessingStrategy {

    /**
     * In a single jvm, kmspan framework can only run as one of The {@link #NRT NRT} and {@link #RT RT} modes.
     * <p>
     * The default mode is the NRT mode.
     * <p>
     * <p>
     * When running in NRT mode:
     * 1. {@link SpanKafkaConsumer#pollWithSpan(long) pollWithSpan} api can not be used,
     * 2. {@link SpanKafkaConsumer#poll(long) poll} api can be used by user code, this method returns
     * {@link org.apache.kafka.clients.consumer.ConsumerRecords ConsumerRecords}, use code must also supply this result
     * to a user defined method that is annotated with {@link org.kmspan.core.annotation.Spaned Spaned} annotation,
     * 3. internally, the {@link SpanKafkaConsumer#poll(long) poll} api still uses the raw
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(long) raw poll} api to poll raw kafka messages off
     * the kafka brokers. For a batch of raw kafka messages, it may contain only user messages or a mix of user messages
     * and span messages. The user messages are not sorted by {@link SpanMessage#getKafkaTimestamp()}. The
     * {@link SpanConstants#SPAN_BEGIN span begin} messages are sorted by {@link SpanMessage#getKafkaTimestamp()}, and
     * are processed (in the same thread) before the execution of the {@link org.kmspan.core.annotation.Spaned Spaned}
     * annotated method. Similarly, {@link SpanConstants#SPAN_END span end} messages are sorted by
     * {@link SpanMessage#getKafkaTimestamp()}, and are processed (in the same thread) after the execution of the
     * {@link org.kmspan.core.annotation.Spaned Spaned} annotated method.
     * <p>
     * When running in RT mode:
     * <p>
     * 1. {@link SpanKafkaConsumer#poll(long) poll} api can not be used,
     * 2. {@link SpanKafkaConsumer#pollWithSpan(long) pollWithSpan} api can be used by user code, this method returns an
     * iterator that enforces user code to walk through the user messages in the order that kmspan wants. Please see
     * {@link SpanProcessingStrategy.Mode#RT rt mode} for more.
     */
    public enum Mode {
        /**
         * the NEAR REAL TIME mode
         */
        NRT("nrt")

        /**
         * the REAL TIME mode
         */
        , RT("rt");

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
         * they will be processed in order of {@link SpanMessage#getKafkaTimestamp()}  generation time}.
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
         * be ordered {@link SpanMessage#getKafkaTimestamp()}  generation time} before being further returned to client
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
