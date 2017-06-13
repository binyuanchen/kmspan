package org.kmspan.core;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.serialization.BaseSpanKeySerializer;

import java.util.*;
import java.util.regex.Pattern;

/**
 * A {@link Consumer consumer} that delegation all communication with Kafka brokers to an internal
 * {@link KafkaConsumer raw consumer}. The raw consumer polls wire Kafka messages whose key are of
 * type {@code SpanKey<K>}, but returns user messages whose key are of type {@code K} to the caller
 * user code. As part of polling raw Kafka messages, span messages are identified and processed,
 * causing span events being generated. Depending on which interface is used, the span messages are
 * processed in two different modes: rough mode and precise mode. For rough mode, please see
 * {@link org.kmspan.core.annotation.Spaned Spaned} annotation, the aspect
 * {@link org.kmspan.core.annotation.SpanedAspect aspect} and the {@link #poll(long) poll} method.
 * For precised mode, please see {@link #pollWithSpan(long) pollWithSpan} method and
 * {@link OrderedMixedIterable iterable}. For more details on both, please see more details on kmspan wiki
 * <a href="https://github.com/binyuanchen/kmspan/wiki">kmspan wiki</a>.
 *
 * @param <K> Type of the key of the user messages
 * @param <V> Type of tge value of the user messages
 */
public class SpanKafkaConsumer<K, V> implements Consumer<K, V> {
    private static Logger logger = LogManager.getLogger(SpanKafkaConsumer.class);

    private KafkaConsumer<SpanKey<K>, V> rawKafkaConsumer;
    private String spanZKQuorum;
    private CuratorFramework curatorFramework;
    private String spanBeginSCZPath;
    private String spanEndSCZPath;
    private SpanProcessingStrategy.Mode processingMode = SpanProcessingStrategy.Mode.ROUGH;

    private KafkaZKSpanEventHandler kafkaZKSpanEventHandler;

    public SpanKafkaConsumer(Map<String, Object> configs, BaseSpanKeySerializer<K> deser) {
        this(configs, deser, null);
    }

    public SpanKafkaConsumer(Map<String, Object> configs, BaseSpanKeySerializer<K> deser, Deserializer<V> valueDeserializer) {
        if (configs.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            throw new IllegalArgumentException(
                    "key " + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + " is not customizable."
            );
        }
        if (configs.containsKey(SpanConstants.SPAN_ZK_QUORUM)) {
            // TODO directly remove or clone a new copy of configs?
            spanZKQuorum = (String) configs.remove(SpanConstants.SPAN_ZK_QUORUM);
        } else {
            spanZKQuorum = (String) configs.remove(SpanConstants.DEFAULT_SPAN_ZK_QUORUM);
        }
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(spanZKQuorum)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();

        if (configs.containsKey(SpanConstants.SPAN_BEGIN_SC_ZPATH)) {
            spanBeginSCZPath = (String) configs.remove(SpanConstants.SPAN_BEGIN_SC_ZPATH);
        } else {
            spanBeginSCZPath = SpanConstants.DEFAULT_SPAN_BEGIN_SC_ZPATH;
        }
        if (configs.containsKey(SpanConstants.SPAN_END_SC_ZPATH)) {
            spanEndSCZPath = (String) configs.remove(SpanConstants.SPAN_END_SC_ZPATH);
        } else {
            spanEndSCZPath = SpanConstants.DEFAULT_SPAN_END_SC_ZPATH;
        }
        if (deser == null) {
            deser = new BaseSpanKeySerializer<>();
        }
        rawKafkaConsumer = new KafkaConsumer<>(configs, deser, valueDeserializer);
        kafkaZKSpanEventHandler = new KafkaZKSpanEventHandler(
                curatorFramework,
                rawKafkaConsumer,
                spanBeginSCZPath,
                spanEndSCZPath);
        String pmodeStr = (String) configs.get(SpanConstants.SPAN_PROCESSING_MODE);
        if (pmodeStr != null) {
            SpanProcessingStrategy.Mode pmode = SpanProcessingStrategy.Mode.getByName(pmodeStr);
            if (pmode == null) {
                throw new IllegalArgumentException("illegal value "
                        + pmodeStr + " is specified for config "
                        + SpanConstants.SPAN_PROCESSING_MODE);
            } else {
                processingMode = pmode;
            }
        } else {
            processingMode = SpanProcessingStrategy.Mode.ROUGH;
        }
    }

    public SpanKafkaConsumer(Properties properties, BaseSpanKeySerializer<K> deser) {
        this(properties, deser, null);
    }

    public SpanKafkaConsumer(Properties properties, BaseSpanKeySerializer<K> deser, Deserializer<V> valueDeserializer) {
        if (properties.contains(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            throw new IllegalArgumentException(
                    "key " + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + " is not customizable."
            );
        }
        if (properties.getProperty(SpanConstants.SPAN_ZK_QUORUM) != null) {
            spanZKQuorum = (String) properties.remove(SpanConstants.SPAN_ZK_QUORUM);
        } else {
            spanZKQuorum = SpanConstants.DEFAULT_SPAN_ZK_QUORUM;
        }
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(spanZKQuorum)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();

        if (properties.get(SpanConstants.SPAN_BEGIN_SC_ZPATH) != null) {
            spanBeginSCZPath = (String) properties.remove(SpanConstants.SPAN_BEGIN_SC_ZPATH);
        } else {
            spanBeginSCZPath = SpanConstants.DEFAULT_SPAN_BEGIN_SC_ZPATH;
        }
        if (properties.get(SpanConstants.SPAN_END_SC_ZPATH) != null) {
            spanEndSCZPath = (String) properties.remove(SpanConstants.SPAN_END_SC_ZPATH);
        } else {
            spanEndSCZPath = SpanConstants.DEFAULT_SPAN_END_SC_ZPATH;
        }
        if (deser == null) {
            deser = new BaseSpanKeySerializer<>();
        }
        rawKafkaConsumer = new KafkaConsumer<>(properties, deser, valueDeserializer);
        kafkaZKSpanEventHandler = new KafkaZKSpanEventHandler(
                curatorFramework,
                rawKafkaConsumer,
                spanBeginSCZPath,
                spanEndSCZPath);
        String pmodeStr = (String) properties.get(SpanConstants.SPAN_PROCESSING_MODE);
        if (pmodeStr != null) {
            SpanProcessingStrategy.Mode pmode = SpanProcessingStrategy.Mode.getByName(pmodeStr);
            if (pmode == null) {
                throw new IllegalArgumentException("illegal value "
                        + pmodeStr + " is specified for config "
                        + SpanConstants.SPAN_PROCESSING_MODE);
            } else {
                processingMode = pmode;
            }
        } else {
            processingMode = SpanProcessingStrategy.Mode.ROUGH;
        }
    }

    public void setProcessingMode(SpanProcessingStrategy.Mode processingMode) {
        this.processingMode = processingMode;
    }

    public void registerSpanEventListener(SpanEventListener listener) {
        this.kafkaZKSpanEventHandler.registerSpanEventListener(listener);
    }

    @Override
    public Set<TopicPartition> assignment() {
        return rawKafkaConsumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return rawKafkaConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        rawKafkaConsumer.subscribe(topics);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        rawKafkaConsumer.subscribe(topics, callback);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        rawKafkaConsumer.assign(partitions);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        rawKafkaConsumer.subscribe(pattern, callback);
    }

    @Override
    public void unsubscribe() {
        rawKafkaConsumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        // if you try to get span messages processed in precise mode, you should not
        // use this api
        if (processingMode.equals(SpanProcessingStrategy.Mode.PRECISE)) {
            throw new IllegalStateException("poll is not supported in span processing mode "
                    + processingMode.getName());
        }
        /**
         * assuming the same caller thread is gonna do the real work synchronously
         * TODO switch mode (if enabled, use annotation, else, process span event right here)
         */
        SpanEventTLHolder.setSpanEventHandler(kafkaZKSpanEventHandler);
        // TODO revisit this logic here: should we clear?
        List<ConsumerSpanEvent> remaining = SpanEventTLHolder.getSpanEvents();
        if (remaining != null && !remaining.isEmpty()) {
            logger.warn("will clear remaining span events = {}", remaining.size());
            remaining.clear();
        }

        ConsumerRecords<SpanKey<K>, V> wireRecords = rawKafkaConsumer.poll(timeout);

        // span messages with some ordering
        TreeSet<ConsumerSpanEvent> newSpanMessages = new TreeSet<>(new SpanMessageComparator());

        // user messages
        Map<TopicPartition, List<ConsumerRecord<K, V>>> newUserRecords = new HashMap<>();

        // collect
        for (TopicPartition partition : wireRecords.partitions()) {
            newUserRecords.put(partition, new ArrayList<>());
            wireRecords.records(partition).stream().forEach(r -> {
                if (r.key().isSpan()) {
                    // TODO switch mode (if enabled, use annotation, else, process span event right here)
                    newSpanMessages.add(SpanMessageUtils.toSpanMessage(r));
                } else {
                    newUserRecords.get(partition).add(SpanMessageUtils.toUserMessage(r));
                }
            });
        }

        SpanEventTLHolder.getSpanEvents().addAll(newSpanMessages);
        return new ConsumerRecords<>(newUserRecords);
    }

    public Iterator<ConsumerRecord<K, V>> pollWithSpan(long timeout) {
        if (processingMode.equals(SpanProcessingStrategy.Mode.ROUGH)) {
            throw new IllegalStateException("pollWithSpan is not supported in span processing mode "
                    + processingMode.getName());
        }

        ConsumerRecords<SpanKey<K>, V> wireRecords = rawKafkaConsumer.poll(timeout);

        return hasAnySpanMessages(wireRecords) ?
                new OrderedMixedIterable<>(kafkaZKSpanEventHandler, wireRecords).iterator() :
                new UserOnlyIterable(wireRecords.iterator()).iterator();
    }

    // decides whether a poll of records contains any span messages
    private boolean hasAnySpanMessages(ConsumerRecords<SpanKey<K>, V> records) {
        return records.partitions().parallelStream().flatMap(tp -> records.records(tp).parallelStream())
                .anyMatch(cr -> cr.key().getType() != null);
    }

    @Override
    public void commitSync() {
        rawKafkaConsumer.commitSync();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        rawKafkaConsumer.commitSync(offsets);
    }

    @Override
    public void commitAsync() {
        rawKafkaConsumer.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        rawKafkaConsumer.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        rawKafkaConsumer.commitAsync(offsets, callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        rawKafkaConsumer.seek(partition, offset);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        rawKafkaConsumer.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        rawKafkaConsumer.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        return rawKafkaConsumer.position(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return rawKafkaConsumer.committed(partition);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return rawKafkaConsumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return rawKafkaConsumer.partitionsFor(topic);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return rawKafkaConsumer.listTopics();
    }

    @Override
    public Set<TopicPartition> paused() {
        return rawKafkaConsumer.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        rawKafkaConsumer.pause(partitions);
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        rawKafkaConsumer.resume(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return rawKafkaConsumer.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return rawKafkaConsumer.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return rawKafkaConsumer.endOffsets(partitions);
    }

    @Override
    public void close() {
        try {
            rawKafkaConsumer.close();
        } finally {
            curatorFramework.close();
        }
    }

    @Override
    public void wakeup() {
        rawKafkaConsumer.wakeup();
    }

    // strictly speaking, the ordering here is very special:
    // -1 no user messages are involved
    // -2 all BEGIN messages are sorted
    // -3 all END messages are sorted
    // -4 no ordering between BEING messages and END messages
    // this is only used to collect span messages and prepare for Spaned annotation processing.
    private static class SpanMessageComparator implements Comparator<ConsumerSpanEvent> {
        // overall, should no return 0, as that will cause lose of events
        @Override
        public int compare(ConsumerSpanEvent o1, ConsumerSpanEvent o2) {
            if (o1.isSpanEvent() || o2.isSpanEvent()) {
                throw new IllegalArgumentException("Expecting only span messages");
            } else {
                if (o1.getSpanEventType().equals(o2.getSpanEventType())) {
                    return o1.getKafkaTimestamp() < o2.getKafkaTimestamp() ? -1 : 1;
                } else {
                    return 1;
                }
            }
        }
    }

    // best-effort comparator for ordering mixed set of user and span messages
    private static class MixedMessageComparator<K, V> implements Comparator<ConsumerRecord<SpanKey<K>, V>> {
        // overall, should no return 0, as that will cause lose of events/messages
        @Override
        public int compare(ConsumerRecord<SpanKey<K>, V> o1, ConsumerRecord<SpanKey<K>, V> o2) {
            if (o1.partition() == o2.partition()) {
                // from same partition, no matter which topic they are in, order by offset
                return o1.offset() < o2.offset() ? -1 : 1;
            } else {
                // from different partitions, lets use timestamp
                if (o1.timestampType().equals(TimestampType.LOG_APPEND_TIME)
                        && o2.timestampType().equals(TimestampType.LOG_APPEND_TIME)) {
                    return o1.timestamp() < o2.timestamp() ? -1 : 1;
                } else if (o1.timestampType().equals(TimestampType.CREATE_TIME)
                        && o2.timestampType().equals(TimestampType.CREATE_TIME)) {
                    // less ideal due to clock differences among clients
                    return o1.timestamp() < o2.timestamp() ? -1 : 1;
                } else {
                    // so bad, let's roughly order them: move span BEGIN messages in front of span END
                    // messages for the same span, regardless of their orders against user messages.
                    SpanKey<K> s1 = o1.key();
                    SpanKey<K> s2 = o2.key();
                    if (s1 == null || s2 == null) {
                        throw new IllegalArgumentException("null message key: o1=" + o1 + ", o2=" + o2);
                    }
                    if (s1.isSpan() && s2.isSpan() && s1.getId().equals(s2.getId())) {
                        return (s1.getType().equals(SpanConstants.SPAN_BEGIN)) ? -1 : 1;
                    } else {
                        return 1; // doesn't matter
                    }
                }
            }
        }
    }

    private class UserOnlyIterable<K, V> implements Iterable<ConsumerRecord<K, V>> {
        private final Iterator<ConsumerRecord<SpanKey<K>, V>> it;

        // assumes input iterator contains only user messages
        public UserOnlyIterable(Iterator<ConsumerRecord<SpanKey<K>, V>> it) {
            this.it = it;
        }

        @Override
        public Iterator<ConsumerRecord<K, V>> iterator() {
            return new AbstractIterator<ConsumerRecord<K, V>>() {
                @Override
                protected ConsumerRecord<K, V> makeNext() {
                    while (it.hasNext()) {
                        SpanMessageUtils.toUserMessage(it.next());
                    }
                    return allDone();
                }
            };
        }
    }

    private class OrderedMixedIterable<K, V> implements Iterable<ConsumerRecord<K, V>> {
        private final SortedSet<ConsumerRecord<SpanKey<K>, V>> sortedSet;
        private final SpanEventHandler spanEventHandler;

        public OrderedMixedIterable(SpanEventHandler spanEventHandler, ConsumerRecords<SpanKey<K>, V> consumerRecords) {
            if (processingMode.equals(SpanProcessingStrategy.Mode.ROUGH)) {
                throw new IllegalStateException("OrderedMixedIterable is not supported in span processing mode "
                        + processingMode.getName());
            }
            this.spanEventHandler = spanEventHandler;

            // use this set to order user and span messages
            this.sortedSet = new TreeSet<>(new MixedMessageComparator<>());

            for (ConsumerRecord<SpanKey<K>, V> consumerRecord : consumerRecords) {
                this.sortedSet.add(consumerRecord);
            }
        }

        @Override
        public Iterator<ConsumerRecord<K, V>> iterator() {
            return new AbstractIterator<ConsumerRecord<K, V>>() {
                Iterator<ConsumerRecord<SpanKey<K>, V>> iter = sortedSet.iterator();

                @Override
                protected ConsumerRecord<K, V> makeNext() {
                    while (iter.hasNext()) {
                        ConsumerRecord<SpanKey<K>, V> record = iter.next();
                        if (record.key().isSpan()) {
                            // span event, process it inline (and in order)
                            spanEventHandler.handle(Arrays.asList(SpanMessageUtils.toSpanMessage(record)));
                        } else {
                            // user message, transform
                            return SpanMessageUtils.toUserMessage(record);
                        }
                    }
                    return allDone();
                }
            };
        }
    }
}
