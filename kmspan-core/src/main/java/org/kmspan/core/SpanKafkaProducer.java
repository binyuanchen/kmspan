package org.kmspan.core;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;
import org.kmspan.core.serialization.BaseSpanKeySerializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Producer producer} that delegates all communication with Kafka brokers to an
 * internal {@link KafkaProducer KafkaProducer}, by converting user messages to a wire format
 * that carries span related information, also with extra APIs to generated span BEGIN and
 * END messages.
 * <p>
 * A span begin or end message is multi-casted to each topic partition of a Kafka topic via
 * {@link #beginSpan(String, String) beginSpan} and {@link #endSpan(String, String) endSpan}
 * methods. See
 * {@link SpanKafkaConsumer SpanKafkaConsumer} and {@link SpanMessageHandler SpanMessageHandler}
 * for how these span messages are collected on consumer side to generate span events.
 */
public class SpanKafkaProducer<K, V> implements Producer<K, V>, SpanMessageTrigger {
    private KafkaProducer<SpanKey<K>, V> rawKafkaProducer;

    public SpanKafkaProducer(Map<String, Object> configs, BaseSpanKeySerializer<K> ser) {
        this(configs, ser, null);
    }

    public SpanKafkaProducer(Map<String, Object> configs, BaseSpanKeySerializer<K> ser, Serializer<V> valueSerializer) {
        if (configs.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            throw new IllegalArgumentException(
                    "key " + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG + " is not customizable.");
        }
        if (ser == null) {
            ser = new BaseSpanKeySerializer<>();
        }
        rawKafkaProducer = new KafkaProducer<>(configs, ser, valueSerializer);
    }

    public SpanKafkaProducer(Properties properties, BaseSpanKeySerializer<K> ser) {
        this(properties, ser, null);
    }

    public SpanKafkaProducer(Properties properties, BaseSpanKeySerializer<K> ser, Serializer<V> valueSerializer) {
        if (properties.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            throw new IllegalArgumentException(
                    "key " + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG + " is not customizable.");
        }
        if (ser == null) {
            ser = new BaseSpanKeySerializer<>();
        }
        rawKafkaProducer = new KafkaProducer<>(properties, ser, valueSerializer);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return rawKafkaProducer.send(
                SpanMessageUtils.toUserMessage(record),
                callback
        );
    }

    @Override
    public void flush() {
        rawKafkaProducer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return rawKafkaProducer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return rawKafkaProducer.metrics();
    }

    @Override
    public void close() {
        rawKafkaProducer.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        rawKafkaProducer.close(timeout, unit);
    }

    @Override
    public void beginSpan(String topic, String spanId) {
        long timestamp = System.currentTimeMillis();
        List<PartitionInfo> partitionInfoList = rawKafkaProducer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfoList) {
            rawKafkaProducer.send(
                    SpanMessageUtils.toSpanMessage(
                            new ProducerRecord<K, V>(topic, partitionInfo.partition(), timestamp, null, null),
                            spanId, SpanConstants.SPAN_BEGIN
                    )
            );
        }
    }

    @Override
    public void endSpan(String topic, String spanId) {
        long timestamp = System.currentTimeMillis();
        List<PartitionInfo> partitionInfoList = rawKafkaProducer.partitionsFor(topic);
        for (PartitionInfo partitionInfo : partitionInfoList) {
            rawKafkaProducer.send(
                    SpanMessageUtils.toSpanMessage(
                            new ProducerRecord<K, V>(topic, partitionInfo.partition(), timestamp, null, null),
                            spanId, SpanConstants.SPAN_END)
            );
        }
    }
}
