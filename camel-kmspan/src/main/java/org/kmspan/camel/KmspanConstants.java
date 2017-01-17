package org.kmspan.camel;

public final class KmspanConstants {

    public static final String PARTITION_KEY = "kafka.PARTITION_KEY";
    public static final String PARTITION = "kafka.PARTITION";
    public static final String KEY = "kafka.KEY";
    public static final String TOPIC = "kafka.TOPIC";
    public static final String OFFSET = "kafka.OFFSET";

    public static final String KAFKA_DEFAULT_ENCODER = "kafka.serializer.DefaultEncoder";
    public static final String KAFKA_STRING_ENCODER = "kafka.serializer.StringEncoder";
    public static final String KAFKA_DEFAULT_SERIALIZER  = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KAFKA_DEFAULT_DESERIALIZER  = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String KAFKA_DEFAULT_PARTITIONER = "org.apache.kafka.clients.producer.internals.DefaultPartitioner";
    public static final String PARTITIONER_RANGE_ASSIGNOR = "org.apache.kafka.clients.consumer.RangeAssignor";
    public static final String KAFKA_RECORDMETA = "org.apache.kafka.clients.producer.RecordMetadata";

    //Span event listener class
    public static final String SPAN_EVENT_LISTEN_REGISTRY_NAME = "span.event.listener";

    private KmspanConstants() {
    }
}
