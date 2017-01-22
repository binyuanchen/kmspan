package org.kmspan.camel;


import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.ConsumerSpanEvent;
import org.kmspan.core.SpanKafkaProducer;
import org.kmspan.testutils.BaseTestUtil;
import org.kmspan.testutils.LocalKafkaBroker;
import org.kmspan.testutils.LocalZookeeperServer;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.mockito.Mockito.*;

public class CamelKmspanTest {
    private static Logger logger = LogManager.getLogger(CamelKmspanTest.class);

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
    public void testSpanIntegrationCamel() {
        final String topicName = BaseTestUtil.generateRandomTopicName();
        logger.info("generated topicName = {}", topicName);
        final String spanId1 = BaseTestUtil.generateRandomSpanId();
        final int numOfMessagesForSpan1 = 10;
        final int numOfPartions = 10;

        BaseTestUtil.createTopic(this.zkServer.getRunningAddr(), topicName, numOfPartions, 1);

        TestUserSpanEventListener targetUserSpanEventListenerSpy = spy(TestUserSpanEventListener.class);

        CamelKafkaConsumerAndListener targetConsumerSpy = spy(CamelKafkaConsumerAndListener.class);
        targetConsumerSpy.setTopic(topicName);
        targetConsumerSpy.setZkServer(this.zkServer);
        targetConsumerSpy.setKafkaBroker(this.kafkaBroker);
        targetConsumerSpy.setTestUserSpanEventListener(targetUserSpanEventListenerSpy);
        targetConsumerSpy.setNumOfPartions(numOfPartions);
        targetConsumerSpy.start();

        // producer
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
            logger.info("\tsending msg {}, {}", msgKey, msgValue);
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

        verify(targetConsumerSpy, times(numOfMessagesForSpan1)).onUserMessage(anyString(), anyString(), anyInt(), anyString());
        verify(targetUserSpanEventListenerSpy, times(2)).onSpanEvent(any(ConsumerSpanEvent.class));
    }

    public static class CamelKafkaConsumerAndListener {
        private String topic;
        private volatile boolean on = false;
        private Thread runner;
        private CamelContext camelContext;
        private LocalKafkaBroker kafkaBroker;
        private LocalZookeeperServer zkServer;
        private TestUserSpanEventListener testUserSpanEventListener;
        private int numOfPartions;

        public void setNumOfPartions(int numOfPartions) {
            this.numOfPartions = numOfPartions;
        }

        public void setTestUserSpanEventListener(TestUserSpanEventListener testUserSpanEventListener) {
            this.testUserSpanEventListener = testUserSpanEventListener;
        }

        public void setKafkaBroker(LocalKafkaBroker kafkaBroker) {
            this.kafkaBroker = kafkaBroker;
        }

        public void setZkServer(LocalZookeeperServer zkServer) {
            this.zkServer = zkServer;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        private void work() {
            SimpleRegistry simpleRegistry = new SimpleRegistry();
            simpleRegistry.put(KmspanConstants.SPAN_EVENT_LISTENER_REGISTRY_NAME, this.testUserSpanEventListener);
            camelContext = new DefaultCamelContext(simpleRegistry);

            try {
                camelContext.addRoutes(
                        new RouteBuilder() {
                            @Override
                            public void configure() throws Exception {

                                String kafkaUri = "kafka:" + kafkaBroker.getRunningAddr()
                                        + "?topic=" + topic
                                        + "&groupId=test&consumersCount=1"
                                        + "&autoOffsetReset=earliest"
                                        + "&keyDeserializer=org.kmspan.camel.TestUserSpanDataStringSerDser"
                                        + "&valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer";

                                String kmspanUri = "kmspan:default"
                                        + "?spanZKQuorum=" + zkServer.getRunningAddr()
                                        + "&spanSCTargetCount=" + numOfPartions;

                                from(kafkaUri)
                                        .to(kmspanUri)
                                        .process(
                                        new Processor() {
                                            @Override
                                            public void process(Exchange exchange) throws Exception {
                                                if (exchange.getIn() != null) {
                                                    Message message = exchange.getIn();
                                                    Long offset = (Long) message.getHeader(KafkaConstants.OFFSET);
                                                    String topic = (String) message.getHeader(KafkaConstants.TOPIC);
                                                    String k = null;
                                                    if (message.getHeader(KafkaConstants.KEY) != null) {
                                                        k = (String) message.getHeader(KafkaConstants.KEY);
                                                    }
                                                    String v = null;
                                                    if (message.getBody() != null) {
                                                        v = (String) message.getBody();
                                                    }
                                                    onUserMessage(k, v, offset, topic);
                                                }
                                            }
                                        }
                                );
                            }
                        }
                );
                camelContext.start();
                logger.info("camelContext started.");
            } catch (Exception e) {
                logger.error("", e);
            }

            while (on) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
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
            try {
                camelContext.stop();
                logger.info("camelContext stopped.");
            } catch (Exception e) {
                logger.error("", e);
            }
        }

        public void onUserMessage(String k, String v, long offset, String topic) {
            logger.info("\t[user] message key={}, val={}, offset={}, topic={}", k, v, offset, topic);
        }
    }
}
