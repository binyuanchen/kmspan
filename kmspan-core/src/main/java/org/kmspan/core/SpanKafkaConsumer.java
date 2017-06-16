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
 * {@link KafkaConsumer raw consumer}. The raw consumer polls raw Kafka messages with key of type {@code SpanKey<K>},
 * and returns user messages with key of type {@code K} to the caller. When polling raw Kafka messages, span
 * messages are identified and processed, causing span events being generated. Depending on the setting of the
 * {@link SpanProcessingStrategy.Mode mode}, different processing methods and constructs should be used.
 * <p>
 * For {@link SpanProcessingStrategy.Mode#NRT NRT mode}, please see {@link org.kmspan.core.annotation.Spaned Spaned}
 * annotation, the {@link org.kmspan.core.annotation.SpanedAspect SpanedAspect} aspect and the
 * {@link #poll(long) poll} method.
 * <p>
 * For {@link SpanProcessingStrategy.Mode#RT RT mode}, please see {@link #pollWithSpan(long) pollWithSpan} method and
 * {@link OrderedMixedIterable iterable}.
 * <p>
 *
 * @param <K> user message key type
 * @param <V> user message value type
 */
public class SpanKafkaConsumer<K, V> implements Consumer<K, V> {
    private static Logger logger = LogManager.getLogger(SpanKafkaConsumer.class);

    private KafkaConsumer<SpanKey<K>, V> rawKafkaConsumer;
    private String spanZKQuorum;
    private CuratorFramework curatorFramework;
    private String spanBeginSCZPath;
    private String spanEndSCZPath;
    private SpanProcessingStrategy.Mode processingMode = SpanProcessingStrategy.Mode.NRT;

    private KafkaZookeeperSpanMessageHandler kafkaZKSpanMessageHandler;

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
        kafkaZKSpanMessageHandler = new KafkaZookeeperSpanMessageHandler(
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
            processingMode = SpanProcessingStrategy.Mode.NRT;
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
        kafkaZKSpanMessageHandler = new KafkaZookeeperSpanMessageHandler(
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
            // default mode is NRT
            processingMode = SpanProcessingStrategy.Mode.NRT;
        }
    }

    public void setProcessingMode(SpanProcessingStrategy.Mode processingMode) {
        this.processingMode = processingMode;
    }

    public void registerSpanEventListener(SpanEventListener listener) {
        this.kafkaZKSpanMessageHandler.registerSpanEventListener(listener);
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

    /**
     * The assumption here is: the current thread (maybe created by the kafka client), a.k.a. the poller thread, is
     * the same thread later that will push (in some framework such as Apache Camel), or return (when client coode
     * directly calls this method then do some processing on the returned messages) the user messages to user code,
     * which will reuse the same thread to process these messages.
     * <p>
     * This api is disabled when kmspan is running in {@link SpanProcessingStrategy.Mode#RT}, the reason is: this method
     * does not process received span messages, it simply does two things (mostly),
     * <p>
     * 1. sort out span messages and put them into thread local,
     * 2. sort out user messages and return them to user code.
     * <p>
     * Because of 1, it is crucial that the user code which receives the returned user messages also passes these messages
     * to a {@link org.kmspan.core.annotation.Spaned annotated} method for further processing.
     *
     * @param timeout timeout waiting for poll result
     * @return
     */
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        if (processingMode.equals(SpanProcessingStrategy.Mode.RT)) {
            throw new IllegalStateException("poll is not supported in span processing mode "
                    + processingMode.getName());
        }
        /**
         * TODO switch mode (if enabled, use annotation, else, process span event right here)
         *
         * TODO Probably does not make sense to register this handler here each time polling, but since the cost is relatively
         * low to do that, I just leave this code here for later to refactor.
         */
        SpanMessageTLHolder.setSpanMessageHandler(kafkaZKSpanMessageHandler);

        /**
         * This is definitely needed as we do not know the current thread is coming from a thread pool or not. If always
         * a new thread is created to poll, thread local will be initialized, but if the current thread is coming from a
         * thread pool, the previous thread local are not 'automatically' cleaned, so we need to clean up it here.
         *
         * Why not letting the code who consumes the thread locals clean up after using? Because there is not single place
         * in the code (or single methods) knows the timing to clean up. See the
         * {@link org.kmspan.core.annotation.SpanedAspect annotation processor}
         */
        List<SpanMessage> remaining = SpanMessageTLHolder.getSpanMessages();
        if (remaining != null && !remaining.isEmpty()) {
            logger.warn("will clear remaining span messages = {}", remaining.size());
            remaining.clear();
        }

        ConsumerRecords<SpanKey<K>, V> wireRecords = rawKafkaConsumer.poll(timeout);

        // span messages with 'some' ordering, please see comparator definition about what 'some' ordering means
        TreeSet<SpanMessage> newSpanMessages = new TreeSet<>(new SpanMessageComparator());

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

        SpanMessageTLHolder.getSpanMessages().addAll(newSpanMessages);
        return new ConsumerRecords<>(newUserRecords);
    }

    /**
     * This method is disabled when kmpsan is running in {@link SpanProcessingStrategy.Mode#NRT}. The reason is: this
     * method polls raw kafka messages and returns an iterator with the mix of span messages and user messages sorted
     * properly.
     *
     * @param timeout timeout waiting for poll response
     * @return
     */
    public Iterator<ConsumerRecord<K, V>> pollWithSpan(long timeout) {
        if (processingMode.equals(SpanProcessingStrategy.Mode.NRT)) {
            throw new IllegalStateException("pollWithSpan is not supported in span processing mode "
                    + processingMode.getName());
        }

        ConsumerRecords<SpanKey<K>, V> wireRecords = rawKafkaConsumer.poll(timeout);

        return hasAnySpanMessages(wireRecords) ?
                new OrderedMixedIterable<>(kafkaZKSpanMessageHandler, wireRecords).iterator() :
                new UserOnlyIterable(wireRecords.iterator()).iterator();
    }

    /**
     * @param records the list of messages to inspect
     * @return true if at least one message is a span message
     */
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

    /**
     * This comparator quickly sorts span messages in this way:
     * <p>
     * -1 no user messages are involved
     * -2 all BEGIN messages are sorted
     * -3 all END messages are sorted
     * -4 no ordering between BEING messages and END messages
     * <p>
     * Purpose: to collect span messages and prepare for {@link org.kmspan.core.annotation.Spaned annotation} processing.
     */
    // strictly speaking, the ordering here is very special:
    //
    private static class SpanMessageComparator implements Comparator<SpanMessage> {
        // overall, should no return 0, as that will cause lose of events
        @Override
        public int compare(SpanMessage o1, SpanMessage o2) {
            if (o1.getSpanEventType().equals(o2.getSpanEventType())) {
                return o1.getKafkaTimestamp() < o2.getKafkaTimestamp() ? -1 : 1;
            } else {
                return 1;
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
                    // bad luck, let's order them heuristically: move span BEGIN messages in front of span END
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
        private final SpanMessageHandler spanMessageHandler;

        public OrderedMixedIterable(SpanMessageHandler spanMessageHandler, ConsumerRecords<SpanKey<K>, V> consumerRecords) {
            if (processingMode.equals(SpanProcessingStrategy.Mode.NRT)) {
                throw new IllegalStateException("OrderedMixedIterable is not supported in span processing mode "
                        + processingMode.getName());
            }
            this.spanMessageHandler = spanMessageHandler;

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
                            spanMessageHandler.handle(Arrays.asList(SpanMessageUtils.toSpanMessage(record)));
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
