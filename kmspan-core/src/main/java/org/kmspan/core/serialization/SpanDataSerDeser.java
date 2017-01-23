package org.kmspan.core.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.kmspan.core.SpanData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The actual messages on the wire (say Kafka) contains
 */
public class SpanDataSerDeser<T> implements Deserializer<SpanData<T>>, Serializer<SpanData<T>> {

    // TODO make it thread safe
    private Kryo kryo;

    public void kryoRegister(Class clazz) {
        kryo.register(clazz);
    }

    public SpanDataSerDeser() {
        kryo = new Kryo();
        kryo.register(SpanData.class);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public SpanData<T> deserialize(String topic, byte[] data) {
        return kryo.readObject(new Input(data), SpanData.class);
    }

    @Override
    public byte[] serialize(String topic, SpanData<T> data) {
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
