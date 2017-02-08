package org.kmspan.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SpanMessageUtils {

    /**
     * convert a span wire message to an user message
     *
     * @param wireMessage
     * @param <K>
     * @param <V>
     * @return null if the wire message is not for user, otherwise a user message
     */
    public static <K, V> ConsumerRecord<K, V> toUserMessage(ConsumerRecord<SpanData<K>, V> wireMessage) {
        if (!wireMessage.key().isSpanMessage()) {
            return new ConsumerRecord<K, V>(wireMessage.topic(), wireMessage.partition(), wireMessage.offset(),
                    wireMessage.key().getData(), wireMessage.value());
        }
        return null;
    }

    public static <K, V> ConsumerSpanEvent toSpanMessage(ConsumerRecord<SpanData<K>, V> wireMessage) {
        if (wireMessage.key().isSpanMessage()) {
            return new ConsumerSpanEvent(wireMessage.key().getSpanId(), wireMessage.key().getSpanEventType(),
                    wireMessage.key().getGenerationTimestamp(), wireMessage.topic());
        }
        return null;
    }
}
