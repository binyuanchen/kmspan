package org.kmspan.core;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/**
 * this sample app demostrates how user code uses {@link SpanKafkaProducer SpanKafkaProducer} to send
 * a 'span' of messages: including span BEGIN message, span END message, and user messages.
 */
public class SampleSpanProducerApp {
    private static Logger logger = LogManager.getLogger(SampleSpanProducerApp.class);
    private final String kafkaBrokerRunningAddr;
    // user intended to send user messages with String key and String value
    private SpanKafkaProducer<String, String> spanKafkaProducer;

    public SampleSpanProducerApp(String kafkaBrokerRunningAddr) {
        this.kafkaBrokerRunningAddr = kafkaBrokerRunningAddr;

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokerRunningAddr);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        // property "key.serializer" is disabled by SpanKafkaConsumer, currently, the key
        // is serialized using the BaseSpanKeySerializer
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.spanKafkaProducer = new SpanKafkaProducer<>(props, null);
    }

    /**
     * a convenient pattern: send a span BEGIN, followed by 'num' user messages, then a span END
     *
     * @param spanId
     * @param topic
     * @param num
     */
    public void beginSpanAndUserMessagesAndEndSpan(String spanId, String topic, int num) {
        beginSpan(spanId, topic);
        sendUserMessages(spanId, topic, num);
        endSpan(spanId, topic);
    }

    /**
     * Send a span BEGIN message
     *
     * @param spanId
     * @param topic
     */
    public void beginSpan(String spanId, String topic) {
        this.spanKafkaProducer.beginSpan(topic, spanId);
    }

    /**
     * Send a span end messages
     *
     * @param spanId
     * @param topic
     */
    public void endSpan(String spanId, String topic) {
        this.spanKafkaProducer.endSpan(topic, spanId);
    }

    /**
     * send 'num' user messages
     *
     * @param spanId
     * @param topic
     * @param num    number of user messages to send
     */
    public void sendUserMessages(String spanId, String topic, int num) {
        for (int i = 0; i < num; i++) {
            String msgKey = String.valueOf(i + 1), msgValue = msgKey;
            logger.debug("\tsending msg {}, {}", msgKey, msgValue);
            this.spanKafkaProducer.send(new ProducerRecord<>(topic, msgKey, msgValue));
        }
    }

    public void flush() {
        this.spanKafkaProducer.flush();
    }

    public void close() {
        this.spanKafkaProducer.close();
    }
}
