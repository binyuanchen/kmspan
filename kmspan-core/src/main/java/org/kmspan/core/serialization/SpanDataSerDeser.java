package org.kmspan.core.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.kmspan.core.SpanKey;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * The actual messages on the wire (say Kafka) contains
 */
public class SpanDataSerDeser<T> implements Deserializer<SpanKey<T>>, Serializer<SpanKey<T>> {

    // TODO make it thread safe
    private Kryo kryo;

    public void kryoRegister(Class clazz) {
        kryo.register(clazz);
    }

    public SpanDataSerDeser() {
        kryo = new Kryo();
        kryo.register(SpanKey.class);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public SpanKey<T> deserialize(String topic, byte[] data) {
        return kryo.readObject(new Input(data), SpanKey.class);
    }

    @Override
    public byte[] serialize(String topic, SpanKey<T> data) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Output output = new Output(baos);
            kryo.writeObject(output, data);
            output.flush();
            return baos.toByteArray();
        }
        catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() {

    }
}
