package org.kmspan.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.precise.SamplePreciseEventModeConsumerApp;
import org.kmspan.core.rough.SampleRoughEventModeConsumerApp;
import org.kmspan.testutils.BaseTestUtil;
import org.kmspan.testutils.LocalKafkaBroker;
import org.kmspan.testutils.LocalZookeeperServer;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class SpanEventBasicCaseUnitTest {
    private static Logger logger = LogManager.getLogger(SpanEventBasicCaseUnitTest.class);

    // a 'local' Zookeeper server that runs in the same jvm as this test
    private LocalZookeeperServer zkServer = new LocalZookeeperServer();

    // a 'local' Kafka broker that runs in the same jvm as this test
    private LocalKafkaBroker kafkaBroker = new LocalKafkaBroker(zkServer.getRunningHost(), zkServer.getRunningPort());

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);

        // run the local zookeeper on a randomly available port
        zkServer = new LocalZookeeperServer("localhost", 0);
        zkServer.startupIfNot();

        // run the local Kafka broker on a randomly available port, connecting to the above local Zookeeper
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
    public void testBasicSpanRoughEventMode() {
        // randomly generate a topic name, for this test
        final String topicName = BaseTestUtil.generateRandomTopicName();
        // the number of partitioned to be configured for the topic
        final int numOfPartitions = 10;

        // randomly generate two span ids, for this test
        final String spanId1 = BaseTestUtil.generateRandomSpanId();
        final String spanId2 = BaseTestUtil.generateRandomSpanId();
        // the number of user messages for each span
        final int num1 = 10;
        final int num2 = 10;

        // create the topic for this test run
        BaseTestUtil.createTopic(zkServer.getRunningAddr(), topicName, numOfPartitions, 1);

        // create a span event listener, and spy on it
        SampleSpanEventListener listenerSpy = spy(SampleSpanEventListener.class);

        // create a consumer that can process span events in rough mode, and spy on it
        SampleRoughEventModeConsumerApp consumerApp = new SampleRoughEventModeConsumerApp(
                zkServer.getRunningAddr(),
                kafkaBroker.getRunningAddr(),
                numOfPartitions,
                topicName,
                listenerSpy);
        SampleRoughEventModeConsumerApp consumerAppSpy = spy(consumerApp);

        // create a prodcuer to send span messages and user messages
        SampleSpanProducerApp producerApp = new SampleSpanProducerApp(kafkaBroker.getRunningAddr());

        // start the consumer polling loop, which runs in a separate thread
        consumerAppSpy.startConsumerLoop();

        // use the producer to send 2 spans of messages, each with 10 user messages
        producerApp.beginSpanAndUserMessagesAndEndSpan(spanId1, topicName, num1);
        producerApp.beginSpanAndUserMessagesAndEndSpan(spanId2, topicName, num2);
        producerApp.flush();
        producerApp.close();

        try {
            // wait sometime before verifying
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
        }
        consumerAppSpy.stopAndWait();

        verify(consumerAppSpy, times(num1 + num2))
                .onUserMessage(anyString(), anyString(), anyInt(), anyString());
        verify(listenerSpy, times(4)).onSpanEvent(any(ConsumerSpanEvent.class));
    }

    @Test
    public void testBasicSpanPreciseEventMode() {
        // randomly generate a topic name, for this test
        final String topicName = BaseTestUtil.generateRandomTopicName();
        // the number of partitioned to be configured for the topic
        final int numOfPartitions = 10;

        // randomly generate two span ids, for this test
        final String spanId1 = BaseTestUtil.generateRandomSpanId();
        final String spanId2 = BaseTestUtil.generateRandomSpanId();
        // the number of user messages for each span
        final int num1 = 10;

        // create the topic for this test run
        BaseTestUtil.createTopic(zkServer.getRunningAddr(), topicName, numOfPartitions, 1);

        // create a span event listener, and spy on it
        SampleSpanEventListener listenerSpy = spy(SampleSpanEventListener.class);

        // create a consumer that can process span events in precise mode, and spy on it
        SamplePreciseEventModeConsumerApp consumerApp = new SamplePreciseEventModeConsumerApp(
                zkServer.getRunningAddr(),
                kafkaBroker.getRunningAddr(),
                numOfPartitions,
                topicName,
                listenerSpy);
        SamplePreciseEventModeConsumerApp consumerAppSpy = spy(consumerApp);

        // create a prodcuer to send span messages and user messages
        SampleSpanProducerApp producerApp = new SampleSpanProducerApp(kafkaBroker.getRunningAddr());

        // start the consumer polling loop, which runs in a separate thread
        consumerAppSpy.startConsumerLoop();

        // use the producer to send 2 spans of messages, each with 10 user messages
        producerApp.beginSpanAndUserMessagesAndEndSpan(spanId1, topicName, num1);
        producerApp.flush();
        producerApp.close();

        try {
            // wait sometime before verifying
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
        }
        consumerAppSpy.stopAndWait();

        verify(consumerAppSpy, times(num1))
                .onUserMessage(anyString(), anyString(), anyInt(), anyString());
        verify(listenerSpy, times(2)).onSpanEvent(any(ConsumerSpanEvent.class));
    }
}
