package org.kmspan.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SpanMessageUtils {

    /**
     * convert a wire message from consumer to a user message
     *
     * @param wireMessage
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> ConsumerRecord<K, V> toUserMessage(ConsumerRecord<SpanKey<K>, V> wireMessage) {
        if (!wireMessage.key().isSpan()) {
            return new ConsumerRecord<>(wireMessage.topic(), wireMessage.partition(), wireMessage.offset(),
                    wireMessage.key().getData(), wireMessage.value());
        }
        return null;
    }

    /**
     * convert a wire message from consumer to a span message (NOT event)
     *
     * @param wireMessage
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> SpanMessage toSpanMessage(ConsumerRecord<SpanKey<K>, V> wireMessage) {
        if (wireMessage.key().isSpan()) {
            return SpanMessage.createSpanMessage(wireMessage.timestampType(),
                    wireMessage.timestamp(), wireMessage.key().getId(),
                    wireMessage.key().getType(), wireMessage.topic());
        }
        return null;
    }

    /**
     * convert a user message to a wire message for producer
     *
     * @param record
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> ProducerRecord<SpanKey<K>, V> toUserMessage(ProducerRecord<K, V> record) {
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                new SpanKey<>(record.key()), record.value()
        );
    }

    public static <K, V> ProducerRecord<SpanKey<K>, V> toSpanMessage(
            ProducerRecord<K, V> record, String spanId, String spanMessageType) {
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                new SpanKey<>(spanId, spanMessageType, record.key()), record.value()
        );
    }
}
