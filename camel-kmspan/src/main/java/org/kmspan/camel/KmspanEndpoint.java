package org.kmspan.camel;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.MultipleConsumersSupport;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.impl.SynchronousDelegateProducer;
import org.apache.camel.spi.ClassResolver;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.util.CastUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UriEndpoint(scheme = "kmspan", title = "Kmspan", syntax = "kmspan:default", producerOnly = true, label = "messaging")
public class KmspanEndpoint extends DefaultEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(KmspanEndpoint.class);

    @UriParam
    private KmspanConfiguration configuration = new KmspanConfiguration();

    public KmspanEndpoint() {
    }

    public KmspanEndpoint(String endpointUri, KmspanComponent component) {
        super(endpointUri, component);
    }

    public KmspanConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(KmspanConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("Not supported for Kmspan consumer creation");
    }

    @Override
    public Producer createProducer() throws Exception {
        return new KmspanProducer(this);
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
