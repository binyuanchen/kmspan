package org.kmspan.core;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.serialization.SpanDataSerDeser;

import java.util.*;
import java.util.regex.Pattern;

/**
 * A {@link Consumer consumer} that delegation all communication with Kafka brokers to an internal
 * {@link KafkaConsumer raw consumer}. The raw consumer polls wire Kafka messages whose key are of
 * type {@code SpanData<K>}, but returns user messages whose key are of type {@code K} to the caller
 * user code. As part of polling raw Kafka messages, span messages are identified and processed,
 * causing span events being generated. Depending on which interface is used, the span messages are
 * processed in two different modes: rough mode and precise mode. For rough mode, please see
 * {@link org.kmspan.core.annotation.Spaned Spaned} annotation, the aspect
 * {@link org.kmspan.core.annotation.SpanedAspect aspect} and the {@link #poll(long) poll} method.
 * For precised mode, please see {@link #pollWithSpan(long) pollWithSpan} method and
 * {@link SpanIterable iterable}. For more details on both, please see more details on kmspan wiki
 * <a href="https://github.com/binyuanchen/kmspan/wiki">kmspan wiki</a>.
 *
 * @param <K> Type of the key of the user messages
 * @param <V> Type of tge value of the user messages
 */
public class SpanKafkaConsumer<K, V> implements Consumer<K, V> {
    private static Logger logger = LogManager.getLogger(SpanKafkaConsumer.class);

    private KafkaConsumer<SpanData<K>, V> rawKafkaConsumer;
    private String spanZKQuorum;
    private CuratorFramework curatorFramework;
    private String spanBeginSCZPath;
    private String spanEndSCZPath;
    private SpanProcessingStrategy.Mode processingMode = SpanProcessingStrategy.Mode.ROUGH;

    private KafkaZKSpanEventHandler kafkaZKSpanEventHandler;

    public SpanKafkaConsumer(Map<String, Object> configs, SpanDataSerDeser<K> deser) {
        this(configs, deser, null);
    }

    public SpanKafkaConsumer(Map<String, Object> configs, SpanDataSerDeser<K> deser, Deserializer<V> valueDeserializer) {
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
            deser = new SpanDataSerDeser<>();
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

    public SpanKafkaConsumer(Properties properties, SpanDataSerDeser<K> deser) {
        this(properties, deser, null);
    }

    public SpanKafkaConsumer(Properties properties, SpanDataSerDeser<K> deser, Deserializer<V> valueDeserializer) {
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
            deser = new SpanDataSerDeser<>();
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

        ConsumerRecords<SpanData<K>, V> wireRecords = rawKafkaConsumer.poll(timeout);

        // internal structure to collect only span events and sort them based on generation timestamp
        TreeSet<ConsumerSpanEvent> sortedConsumerSpanEvents = new TreeSet<ConsumerSpanEvent>(new Comparator<ConsumerSpanEvent>() {
            @Override
            public int compare(ConsumerSpanEvent o1, ConsumerSpanEvent o2) {
                if (o1.getGenerationTime() < o2.getGenerationTime()) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });

        // records to be return to user functions
        Map<TopicPartition, List<ConsumerRecord<K, V>>> newUserRecords = new HashMap<>();
        for (TopicPartition partition : wireRecords.partitions()) {
            newUserRecords.put(partition, new ArrayList<>());
            List<ConsumerRecord<SpanData<K>, V>> partitionRecords = wireRecords.records(partition);
            for (ConsumerRecord<SpanData<K>, V> partitionRecord : partitionRecords) {
                SpanData<K> spanKey = partitionRecord.key();
                if (spanKey.getSpanEventType() != null) {
                    // TODO switch mode (if enabled, use annotation, else, process span event right here)
                    sortedConsumerSpanEvents.add(new ConsumerSpanEvent(
                            spanKey.getSpanId(),
                            spanKey.getSpanEventType(),
                            spanKey.getGenerationTimestamp(),
                            partitionRecord.topic()));
                } else {
                    newUserRecords.get(partition).add(
                            new ConsumerRecord<K, V>(
                                    partitionRecord.topic(),
                                    partitionRecord.partition(),
                                    partitionRecord.offset(),
                                    spanKey.getData(),
                                    partitionRecord.value()
                            )
                    );
                }
            }
        }
        // adding all events to list maintaining their order
        SpanEventTLHolder.getSpanEvents().addAll(sortedConsumerSpanEvents);
        return new ConsumerRecords<>(newUserRecords);
    }

    public Iterator<ConsumerRecord<K, V>> pollWithSpan(long timeout) {
        if (processingMode.equals(SpanProcessingStrategy.Mode.ROUGH)) {
            throw new IllegalStateException("pollWithSpan is not supported in span processing mode "
                    + processingMode.getName());
        }

        ConsumerRecords<SpanData<K>, V> wireRecords = rawKafkaConsumer.poll(timeout);

        return hasAnySpanMessages(wireRecords) ?
                new SpanIterable<>(kafkaZKSpanEventHandler, wireRecords).iterator() :
                new UserOnlyIterable(wireRecords.iterator()).iterator();
    }

    // decides whether a poll of records contains any span messages
    private boolean hasAnySpanMessages(ConsumerRecords<SpanData<K>, V> records) {
        return records.partitions().parallelStream().flatMap(tp -> records.records(tp).parallelStream())
                .anyMatch(cr -> cr.key().getSpanEventType() != null);
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

    private class UserOnlyIterable<K, V> implements Iterable<ConsumerRecord<K, V>> {
        private final Iterator<ConsumerRecord<SpanData<K>, V>> it;

        // assumes input iterator contains only user messages
        public UserOnlyIterable(Iterator<ConsumerRecord<SpanData<K>, V>> it) {
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

    private class SpanIterable<K, V> implements Iterable<ConsumerRecord<K, V>> {
        private final SortedSet<ConsumerRecord<SpanData<K>, V>> sortedSet;
        private final SpanEventHandler spanEventHandler;

        public SpanIterable(SpanEventHandler spanEventHandler, ConsumerRecords<SpanData<K>, V> consumerRecords) {
            if (processingMode.equals(SpanProcessingStrategy.Mode.ROUGH)) {
                throw new IllegalStateException("SpanIterable is not supported in span processing mode "
                        + processingMode.getName());
            }
            this.spanEventHandler = spanEventHandler;

            // comparator
            this.sortedSet = new TreeSet<>((ConsumerRecord<SpanData<K>, V> o1, ConsumerRecord<SpanData<K>, V> o2) -> {
                SpanData<K> sad1 = o1.key();
                SpanData<K> sad2 = o2.key();
                if (sad1 != null && sad2 != null) {
                    if (sad1.getGenerationTimestamp() < sad2.getGenerationTimestamp()) {
                        return -1;
                    } else if (sad1.getGenerationTimestamp() > sad2.getGenerationTimestamp()) {
                        return 1;
                    } else {
                        TopicPartition tp1 = new TopicPartition(o1.topic(), o1.partition());
                        TopicPartition tp2 = new TopicPartition(o2.topic(), o2.partition());
                        if (tp1.equals(tp2)) {
                            if (o1.offset() < o2.offset()) {
                                return -1;
                            } else {
                                return 1;
                            }
                        } else {
                            // not from same TP, and with same event generation timestamp
                            // always put span begin event in front of span end event for the same span
                            if (sad1.getSpanId() != null
                                    && !sad1.getSpanId().equals(sad2.getSpanId())
                                    && sad1.getSpanEventType() != null
                                    && sad2.getSpanEventType() != null
                                    && !sad1.getSpanEventType().equals(sad2.getSpanEventType())) {
                                if (sad1.getSpanEventType().equals(SpanConstants.SPAN_BEGIN)) {
                                    return -1;
                                } else {
                                    return 1;
                                }
                            } else {
                                return 1;
                            }
                        }
                    }
                } else {
                    throw new IllegalArgumentException("missing message key: o1=" + o1 + ", o2=" + o2);
                }
            });
            for (ConsumerRecord<SpanData<K>, V> consumerRecord : consumerRecords) {
                this.sortedSet.add(consumerRecord);
            }
        }

        @Override
        public Iterator<ConsumerRecord<K, V>> iterator() {
            return new AbstractIterator<ConsumerRecord<K, V>>() {
                Iterator<ConsumerRecord<SpanData<K>, V>> iter = sortedSet.iterator();

                @Override
                protected ConsumerRecord<K, V> makeNext() {
                    while (iter.hasNext()) {
                        ConsumerRecord<SpanData<K>, V> record = iter.next();
                        if (record.key().isSpanMessage()) {
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
