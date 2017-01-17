package org.kmspan.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.annotation.Spaned;
import org.kmspan.testutils.BaseTestUtil;
import org.kmspan.testutils.LocalKafkaBroker;
import org.kmspan.testutils.LocalZookeeperServer;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;

public class SpanEventTest {
    private static Logger logger = LogManager.getLogger(SpanEventTest.class);

    // servers with default values
    private LocalZookeeperServer zkServer = new LocalZookeeperServer();
    private LocalKafkaBroker kafkaBroker = new LocalKafkaBroker(zkServer.getRunningHost(), zkServer.getRunningPort());

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);

        zkServer = new LocalZookeeperServer("localhost", 0);
        zkServer.startupIfNot();
        kafkaBroker = new LocalKafkaBroker(zkServer.getRunningHost(), zkServer.getRunningPort(), "localhost", 0);
        kafkaBroker.startupIfNot();
    }

    @AfterClass
    public void cleanup() {
        try {
            kafkaBroker.shutdown();
        } finally {
            zkServer.shutdown();
        }
    }

    @Test
    public void testSendReceiveSpanEventsUsingSpanedAnnotation() {
        // spanId(s) and topicName should be different for each test case and its run
        final String topicName = BaseTestUtil.generateRandomTopicName();
        logger.info("generated topicName = {}", topicName);
        final String spanId1 = BaseTestUtil.generateRandomSpanId();
        final String spanId2 = BaseTestUtil.generateRandomSpanId();
        // this test will send 2 spans of data
        final int numOfMessagesForSpan1 = 10;
        final int numOfMessagesForSpan2 = 10;
        final int numOfPartitions = 10;

        // create the topic for this test run in the test kafka broker
        BaseTestUtil.createTopic(zkServer.getRunningAddr(), topicName, numOfPartitions, 1);

//        BaseTestUtil.applyWorkaround(LocalKafkaBroker.getRunningAddr());

        // this consumer is the real target of testing.
        RoughSpanConsumerAndListener targetConsumerSpy = spy(RoughSpanConsumerAndListener.class);
        targetConsumerSpy.setTopic(topicName);
        targetConsumerSpy.setNumOfPartitions(numOfPartitions);
        targetConsumerSpy.setZkServer(this.zkServer);
        targetConsumerSpy.setKafkaBroker(this.kafkaBroker);
        targetConsumerSpy.start();

        // producer
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBroker.getRunningAddr());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        SpanKafkaProducer<String, String> producer = new SpanKafkaProducer<>(props, null);
        // send span 1 (begin, messages, and end)
        producer.beginSpan(topicName, spanId1);
        logger.info("producer begin span {}", spanId1);
        for (int i = 0; i < numOfMessagesForSpan1; i++) {
            String msgKey = String.valueOf(i + 1), msgValue = msgKey;
            logger.debug("\tsending msg {}, {}", msgKey, msgValue);
            producer.send(new ProducerRecord<>(topicName, msgKey, msgValue));
        }
        producer.endSpan(topicName, spanId1);
        logger.info("producer end span {}", spanId1);

        // send span 2
        producer.beginSpan(topicName, spanId2);
        logger.info("producer begin span {}", spanId2);
        for (int i = 0; i < numOfMessagesForSpan2; i++) {
            String msgKey = String.valueOf(i + 1), msgValue = msgKey;
            logger.debug("\tsending msg {}, {}", msgKey, msgValue);
            producer.send(new ProducerRecord<>(topicName, msgKey, msgValue));
        }
        producer.endSpan(topicName, spanId2);
        logger.info("producer end span {}", spanId2);

        producer.flush();
        producer.close();

        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
        }

        targetConsumerSpy.stopAndWait();

        verify(targetConsumerSpy, times(numOfMessagesForSpan1 + numOfMessagesForSpan2))
                .onUserMessage(anyString(), anyString(), anyInt(), anyString());
        verify(targetConsumerSpy, times(4))
                .onSpanEvent(any(ConsumerSpanEvent.class));
    }

    @Test
    public void testSendReceiveSpanEventsUsingSpanIterator() {
        final String topicName = BaseTestUtil.generateRandomTopicName();
        logger.info("generated topicName = {}", topicName);
        final String spanId1 = BaseTestUtil.generateRandomSpanId();
        final int numOfMessagesForSpan1 = 10;
        final int numOfPartitions = 10;

        boolean suc = false;
        while (!suc) {
            try {
                BaseTestUtil.createTopic(this.zkServer.getRunningAddr(), topicName, numOfPartitions, 1);
                suc = true;
            } catch (Exception e) {
                logger.error("failed to create topic {}", topicName);
            }
        }

//        BaseTestUtil.applyWorkaround(LocalKafkaBroker.getRunningAddr());

        IterableSpanConsumerAndListener targetConsumerSpy = spy(IterableSpanConsumerAndListener.class);
        targetConsumerSpy.setTopic(topicName);
        targetConsumerSpy.setNumOfPartitions(numOfPartitions);
        targetConsumerSpy.setZkServer(this.zkServer);
        targetConsumerSpy.setKafkaBroker(this.kafkaBroker);
        targetConsumerSpy.start();

        Properties props = new Properties();
        props.put("bootstrap.servers", this.kafkaBroker.getRunningAddr());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        SpanKafkaProducer<String, String> producer = new SpanKafkaProducer<>(props, null);
        // send span 1 (begin, messages, and end)
        producer.beginSpan(topicName, spanId1);
        logger.info("producer begin span {}", spanId1);
        for (int i = 0; i < numOfMessagesForSpan1; i++) {
            String msgKey = String.valueOf(i + 1), msgValue = msgKey;
            logger.debug("\tsending msg {}, {}", msgKey, msgValue);
            producer.send(new ProducerRecord<>(topicName, msgKey, msgValue));
        }
        producer.endSpan(topicName, spanId1);
        logger.info("producer end span {}", spanId1);
        producer.flush();
        producer.close();

        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
        }

        targetConsumerSpy.stopAndWait();

        // verify
        verify(targetConsumerSpy, times(numOfMessagesForSpan1)).
                onUserMessage(anyString(), anyString(), anyInt(), anyString());
        verify(targetConsumerSpy, times(2)).onSpanEvent(any(ConsumerSpanEvent.class));
    }

    // this is the test target consumer: it coordinates span events processing and user
    // messages in batch, and not expecting very accurate real-time processing of span
    // events interleaved with user messages, pay attention to the use of @Spaned annotation.
    public static class RoughSpanConsumerAndListener implements SpanEventListener {
        private String topic;
        private volatile boolean on = false;
        private Thread runner;
        private SpanKafkaConsumer<String, String> consumer;
        private int numOfPartitions;
        private LocalKafkaBroker kafkaBroker;
        private LocalZookeeperServer zkServer;

        public void setKafkaBroker(LocalKafkaBroker kafkaBroker) {
            this.kafkaBroker = kafkaBroker;
        }

        public void setZkServer(LocalZookeeperServer zkServer) {
            this.zkServer = zkServer;
        }

        public void setNumOfPartitions(int numOfPartitions) {
            this.numOfPartitions = numOfPartitions;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        private void work() {
            Properties props = new Properties();
            props.put("bootstrap.servers", this.kafkaBroker.getRunningAddr());
            props.put("group.id", "test");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            //
            props.put(SpanConstants.SPAN_ZK_QUORUM, this.zkServer.getRunningAddr());
            props.put(SpanConstants.SPAN_BEGIN_SC_ZPATH, SpanConstants.DEFAULT_SPAN_BEGIN_SC_ZPATH);
            props.put(SpanConstants.SPAN_END_SC_ZPATH, SpanConstants.DEFAULT_SPAN_END_SC_ZPATH);

            this.consumer = new SpanKafkaConsumer<>(props, null);
//            this.consumer.subscribe(Arrays.asList(topic));

            List<TopicPartition> partitions = new ArrayList<>();
            for (int i = 0; i < this.numOfPartitions; i++) {
                partitions.add(new TopicPartition(topic, i));
            }
            this.consumer.assign(partitions);
            this.consumer.seekToBeginning(partitions);

            // this consumer is also the span event listener
            this.consumer.registerSpanEventListener(this);

            logger.info("consumer will start polling");
            int total = 0;
            while (on) {
                ConsumerRecords<String, String> records = this.consumer.poll(100);
                int delta = records.count();
                total += delta;

                onUserMessages(records);
                if (delta > 0) {
                    logger.info("\ttotal user msgs = {}", total);
                }
            }
        }

        public void start() {
            on = true;
            this.runner = new Thread() {
                @Override
                public void run() {
                    work();
                }
            };
            this.runner.start();
        }

        public void stopAndWait() {
            on = false;
            try {
                runner.join();
            } catch (InterruptedException e) {
                throw new IllegalStateException("consumer runner thread is interrupted.");
            }
            this.consumer.close();
        }

        @Spaned
        public void onUserMessages(ConsumerRecords<String, String> records) {
            //logger.info("\t[user]consumer onUserMessages");
            for (ConsumerRecord<String, String> record : records) {
                onUserMessage(record.key(), record.value(), record.offset(), record.topic());
            }
        }

        // the span of this consumer will be interrogated for the call of this method
        public void onUserMessage(String k, String v, long offset, String topic) {
            logger.debug("\t[user]consumer onUserMessage key={}, val={}, offset={}, topic={}", k, v, offset, topic);
        }

        // the span of this consumer will be interrogated for the call of this method
        @Override
        public void onSpanEvent(ConsumerSpanEvent event) {
            logger.info("[span]consumer received span event: {}", event);
        }
    }

    public static class IterableSpanConsumerAndListener implements SpanEventListener {
        private String topic;
        private volatile boolean on = false;
        private Thread runner;
        private SpanKafkaConsumer<String, String> consumer;
        private int numOfPartitions;
        private LocalKafkaBroker kafkaBroker;
        private LocalZookeeperServer zkServer;

        public void setKafkaBroker(LocalKafkaBroker kafkaBroker) {
            this.kafkaBroker = kafkaBroker;
        }

        public void setZkServer(LocalZookeeperServer zkServer) {
            this.zkServer = zkServer;
        }

        public void setNumOfPartitions(int numOfPartitions) {
            this.numOfPartitions = numOfPartitions;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        private void work() {
            Properties props = new Properties();
            props.put("bootstrap.servers", this.kafkaBroker.getRunningAddr());
            props.put("group.id", "test");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            //
            props.put(SpanConstants.SPAN_ZK_QUORUM, this.zkServer.getRunningAddr());
            props.put(SpanConstants.SPAN_BEGIN_SC_ZPATH, SpanConstants.DEFAULT_SPAN_BEGIN_SC_ZPATH);
            props.put(SpanConstants.SPAN_END_SC_ZPATH, SpanConstants.DEFAULT_SPAN_END_SC_ZPATH);

            this.consumer = new SpanKafkaConsumer<>(props, null);
//            this.consumer.subscribe(Arrays.asList(topic));
            List<TopicPartition> partitions = new ArrayList<>();
            for (int i = 0; i < this.numOfPartitions; i++) {
                partitions.add(new TopicPartition(topic, i));
            }
            this.consumer.assign(partitions);
            this.consumer.seekToBeginning(partitions);

            this.consumer.registerSpanEventListener(this);

            int total = 0;
            while (on) {
                Iterator<ConsumerRecord<String, String>> it = consumer.pollWithSpan(100);
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

        public void start() {
            on = true;
            this.runner = new Thread(new Runnable() {
                @Override
                public void run() {
                    work();
                }
            });
            this.runner.start();
        }

        public void stopAndWait() {
            on = false;
            try {
                runner.join();
            } catch (InterruptedException e) {
                throw new IllegalStateException("consumer runner thread is interrupted.");
            }
            this.consumer.close();
        }

        public void onUserMessage(String k, String v, long offset, String topic) {
            logger.info("\t[user]consumer received message key={}, val={}, offset={}, topic={}", k, v, offset, topic);
        }

        @Override
        public void onSpanEvent(ConsumerSpanEvent event) {
            logger.info("[span]consumer received span event: {}", event);
        }
    }
}
