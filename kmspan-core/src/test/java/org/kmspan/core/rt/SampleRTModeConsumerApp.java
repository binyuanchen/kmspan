package org.kmspan.core.rt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.SpanConstants;
import org.kmspan.core.SpanEventListener;
import org.kmspan.core.SpanKafkaConsumer;
import org.kmspan.core.annotation.Spaned;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * This sample app demonstrates how user code uses {@link SpanKafkaConsumer SpanKafkaConsumer} to (1) consume
 * (from Kafka brokers) user messages (both message key and message value are of type String) in a span,
 * and (2) get notified when span events are generated from the stream of span messages. The span events
 * generated here is using rt event mode, for this, please pay attention to the use of
 * {@link SpanKafkaConsumer#pollWithSpan(long) pollWithSpan(long)} method inside the
 * {@link #pollingLoopThread pollingLoopThread}'s {@link Thread#run() run()} method, and the
 * {@link org.kmspan.core.SpanKafkaConsumer.SpanIterable OrderedMixedIterable} returned by that method.
 */
public class SampleRTModeConsumerApp {
    private static Logger logger = LogManager.getLogger(SampleRTModeConsumerApp.class);

    private final String topic;
    private final int numOfPartitions;
    private final String kafkaBrokerRunningAddr;
    private final String zkServerRunningAddr;
    SpanEventListener spanEventListener;
    // this is use to start and stop this consumer during unit test
    private volatile boolean on = false;
    // internally, this consumer is used to poll Kafka brokers and process span messages, the types
    // <String, String> indicates that the user intends to send and consumer Kafka messages whose
    // key and value are of type String
    private SpanKafkaConsumer<String, String> spanKafkaConsumer;
    // the polling is done periodically inside a separate thread
    private Thread pollingLoopThread;


    /**
     * @param zkServerRunningAddr The Zookeeper quorum location that kmspan will use
     * @param kafkaBrokerRunning  The Kafka brokers location that the {@link #spanKafkaConsumer spanKafkaConsumer}
     *                            will use
     * @param numOfPartitions     The total topic partitions for the Kafka {@link #topic}
     * @param topic               The topic that the {@link #spanKafkaConsumer spanKafkaConsumer} will poll from
     * @param spanEventListener   A listener that user passed in to get notified on span events, if any
     */
    public SampleRTModeConsumerApp(String zkServerRunningAddr,
                                   String kafkaBrokerRunning,
                                   int numOfPartitions,
                                   String topic,
                                   SpanEventListener spanEventListener) {
        this.zkServerRunningAddr = zkServerRunningAddr;
        this.kafkaBrokerRunningAddr = kafkaBrokerRunning;
        this.numOfPartitions = numOfPartitions;
        this.topic = topic;
        this.spanEventListener = spanEventListener;
    }

    /**
     * For ease of test, this method is used to start the spanKafkaConsumer polling loop.
     * A new thread is spawned to run the spanKafkaConsumer polling loop. Notice the fact
     * that the polling is done using {@link SpanKafkaConsumer#poll(long)} makes the span
     * events being generate in 'nrt' mode.
     */
    public void startConsumerLoop() {
        this.on = true;
        this.pollingLoopThread = new Thread() {
            @Override
            public void run() {
                // some basic properties
                Properties props = new Properties();
                props.put("bootstrap.servers", kafkaBrokerRunningAddr);
                props.put("group.id", "test");
                props.put("enable.auto.commit", "true");
                props.put("auto.commit.interval.ms", "200");
                // property "key.deserializer" is disabled by SpanKafkaConsumer, currently, the key
                // is deserialized using the BaseSpanKeySerializer
                props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                // kmspan uses this zookeeper for storage of shared counter
                props.put(SpanConstants.SPAN_ZK_QUORUM, zkServerRunningAddr);
                // the shared counter path for the span BEGIN, this is a path prefix
                props.put(SpanConstants.SPAN_BEGIN_SC_ZPATH, SpanConstants.DEFAULT_SPAN_BEGIN_SC_ZPATH);
                // the shared counter path for the span END, this is a path prefix
                props.put(SpanConstants.SPAN_END_SC_ZPATH, SpanConstants.DEFAULT_SPAN_END_SC_ZPATH);
                // set the span messages processing mode to RT
                props.put(SpanConstants.SPAN_PROCESSING_MODE, "rt");

                // create a spanKafkaConsumer
                spanKafkaConsumer = new SpanKafkaConsumer<>(props, null);

                // assign all partitions to this spanKafkaConsumer, so we have a single consumer
                // thread that consumes from all partitions of this topic
                List<TopicPartition> partitions = new ArrayList<>();
                for (int i = 0; i < numOfPartitions; i++) {
                    partitions.add(new TopicPartition(topic, i));
                }
                spanKafkaConsumer.assign(partitions);
                spanKafkaConsumer.seekToBeginning(partitions);

                // you can register span event listener with spanKafkaConsumer, it will callback
                // the registered listener on generating each span event
                spanKafkaConsumer.registerSpanEventListener(spanEventListener);

                logger.info("consumer will start polling loop");
                int total = 0;
                while (on) {
                    // it is important to use pollWithSpan() instead of the regular poll(), then
                    // iterate through each ConsumerRecord as below. Because as you iterate through
                    // each of the record from this iterator, behind the scenes, span messages will
                    // be processed and span events are generated.
                    Iterator<ConsumerRecord<String, String>> it = spanKafkaConsumer.pollWithSpan(100);
                    int delta = 0;
                    while (it.hasNext()) {
                        delta++;
                        ConsumerRecord<String, String> record = it.next();
                        onUserMessage(record.key(), record.value(), record.offset(), record.topic());
                    }
                    total += delta;
                    if (delta > 0) {
                        logger.info("\ttotal user msgs = {}", total);
                    }
                }
            }
        };
        this.pollingLoopThread.start();
    }

    /**
     * For ease of test, this method is used to break the spanKafkaConsumer polling loop and close the
     * spanKafkaConsumer, and wait for these operations to finish. After this call, this app is no longer
     * usable anymore.
     */
    public void stopAndWait() {
        this.on = false;
        try {
            this.pollingLoopThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.spanKafkaConsumer.close();
    }

    /**
     * This method is called for each poll result
     *
     * @param records
     */
    @Spaned
    public void onUserMessages(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            onUserMessage(record.key(), record.value(), record.offset(), record.topic());
        }
    }

    /**
     * This method is called for each user message. In the unit test, this method is spied.
     *
     * @param k
     * @param v
     * @param offset
     * @param topic
     */
    public void onUserMessage(String k, String v, long offset, String topic) {
        logger.debug("\t[user]consumer onUserMessage key={}, val={}, offset={}, topic={}", k, v, offset, topic);
    }
}
