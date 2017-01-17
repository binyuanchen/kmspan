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

@UriEndpoint(scheme = "kmspan", title = "Kmspan", syntax = "kmspan:brokers", consumerClass = KmspanConsumer.class, label = "messaging")
public class KmspanEndpoint extends DefaultEndpoint implements MultipleConsumersSupport {
    private static final Logger LOG = LoggerFactory.getLogger(KmspanEndpoint.class);
    
    @UriParam
    private KmspanConfiguration configuration = new KmspanConfiguration();
    @UriParam(label = "producer")
    private boolean bridgeEndpoint;

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
        KmspanConsumer consumer = new KmspanConsumer(this, processor);
        configureConsumer(consumer);
        return consumer;
    }

    @Override
    public Producer createProducer() throws Exception {
        KmspanProducer producer = createProducer(this);
        if (isSynchronous()) {
            return new SynchronousDelegateProducer(producer);
        } else {
            return producer;
        }
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public boolean isMultipleConsumersSupported() {
        return true;
    }

    private void loadParitionerClass(ClassResolver resolver, Properties props) {
        replaceWithClass(props, "partitioner.class", resolver, Partitioner.class);
    }
    <T> Class<T> loadClass(Object o, ClassResolver resolver, Class<T> type) {
        if (o == null || o instanceof Class) {
            return CastUtils.cast((Class<?>)o);
        }
        String name = o.toString();
        Class<T> c = resolver.resolveClass(name, type);
        if (c == null) {
            c = resolver.resolveClass(name, type, getClass().getClassLoader());
        }
        if (c == null) {
            c = resolver.resolveClass(name, type, org.apache.kafka.clients.producer.KafkaProducer.class.getClassLoader());
        }
        return c;
    }

    void replaceWithClass(Properties props, String key,  ClassResolver resolver, Class<?> type) {
        Class<?> c = loadClass(props.get(key), resolver, type);
        if (c != null) {
            props.put(key, c);
        }
    }

    public void updateClassProperties(Properties props) {
        try {
            if (getCamelContext() != null) {
                ClassResolver resolver = getCamelContext().getClassResolver();
                replaceWithClass(props, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, resolver, Serializer.class);
                replaceWithClass(props, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, resolver, Serializer.class);
                replaceWithClass(props, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, resolver, Deserializer.class);
                replaceWithClass(props, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, resolver, Deserializer.class);
                
                try {
                    //doesn't exist in old version of Kafka client so detect and only call the method if
                    //the field/config actually exists
                    Field f = ProducerConfig.class.getDeclaredField("PARTITIONER_CLASS_CONFIG");
                    if (f != null) {
                        loadParitionerClass(resolver, props);
                    }
                } catch (NoSuchFieldException e) {
                    //ignore
                } catch (SecurityException e) {
                    //ignore
                }
                //doesn't work as it needs to be List<String>  :(
                //replaceWithClass(props, "partition.assignment.strategy", resolver, PartitionAssignor.class);
            }
        } catch (Throwable t) {
            //can ignore and Kafka itself might be able to handle it, if not, it will throw an exception
            LOG.debug("Problem loading classes for Serializers", t);
        }
    }
    
    public ExecutorService createExecutor() {
        return getCamelContext().getExecutorServiceManager().newFixedThreadPool(this, "KmspanConsumer[" + configuration.getTopic() + "]", configuration.getConsumerStreams());
    }

    public ExecutorService createProducerExecutor() {
        int core = getConfiguration().getWorkerPoolCoreSize();
        int max = getConfiguration().getWorkerPoolMaxSize();
        return getCamelContext().getExecutorServiceManager().newThreadPool(this, "KmspanProducer[" + configuration.getTopic() + "]", core, max);
    }

    public Exchange createKafkaExchange(ConsumerRecord record) {
        Exchange exchange = super.createExchange();

        Message message = exchange.getIn();
        message.setHeader(KmspanConstants.PARTITION, record.partition());
        message.setHeader(KmspanConstants.TOPIC, record.topic());
        message.setHeader(KmspanConstants.OFFSET, record.offset());
        if (record.key() != null) {
            message.setHeader(KmspanConstants.KEY, record.key());
        }
        message.setBody(record.value());

        return exchange;
    }

    protected KmspanProducer createProducer(KmspanEndpoint endpoint) {
        return new KmspanProducer(endpoint);
    }

    public boolean isBridgeEndpoint() {
        return bridgeEndpoint;
    }

    /**
     * If the option is true, then KmspanProducer will ignore the KmspanConstants.TOPIC header setting of the inbound message.
     */
    public void setBridgeEndpoint(boolean bridgeEndpoint) {
        this.bridgeEndpoint = bridgeEndpoint;
    }
}
