package org.kmspan.core.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.kmspan.core.SpanKey;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The base serializer and de-serializer template of {@link SpanKey span key} who carries user message key of
 * type {@link T}. For example of using this template, see tests of this class.
 */
public class BaseSpanKeySerializer<T> implements Deserializer<SpanKey<T>>, Serializer<SpanKey<T>> {

    private final static ConcurrentMap<Class, Boolean> clazzes = new ConcurrentHashMap<>();

    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            synchronized (clazzes) {
                for (Class c : clazzes.keySet()) {
                    kryo.register(c);
                }
            }
            return kryo;
        }
    };

    public BaseSpanKeySerializer() {
        clazzes.putIfAbsent(SpanKey.class, Boolean.TRUE);
    }

    public void kryoRegisters(List<Class> clazzList) {
        synchronized (clazzes) {
            clazzList.forEach(clazz -> {
                clazzes.putIfAbsent(clazz, Boolean.TRUE);
            });
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public SpanKey<T> deserialize(String topic, byte[] data) {
        return kryos.get().readObject(new Input(data), SpanKey.class);
    }

    @Override
    public byte[] serialize(String topic, SpanKey<T> data) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Output output = new Output(baos);
            kryos.get().writeObject(output, data);
            output.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void close() {

    }
}
