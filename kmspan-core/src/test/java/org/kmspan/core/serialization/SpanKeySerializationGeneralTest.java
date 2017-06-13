package org.kmspan.core.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.SpanKey;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class SpanKeySerializationGeneralTest {
    private static Logger logger = LogManager.getLogger(SpanKeySerializationGeneralTest.class);

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterClass
    public void cleanup() {
    }

    private void verifySpanData(SpanKey actual, SpanKey expected) {
        Assert.assertNotNull(actual);
        Assert.assertNotNull(expected);
        Assert.assertEquals(actual.getId(), expected.getId());
        Assert.assertEquals(actual.getType(), expected.getType());
        Assert.assertEquals(actual.getData(), expected.getData());
    }

    @Test
    public void testSerWithClassWritten() {
        UserKeyMetaValue metaValue = new UserKeyMetaValue();
        metaValue.setValue("a");
        Map<String, UserKeyMetaValue> metaValueMap = new HashMap<>();
        metaValueMap.put("b", metaValue);
        UserKey userKey1 = new UserKey();
        userKey1.setKeyId("c");
        userKey1.setMeta(metaValueMap);
        SpanKey<UserKey> spanKey = new SpanKey<>("d", "e", userKey1);

        byte[] persisted = null;
        Kryo kryo1 = new Kryo();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Output output = new Output(baos);
            kryo1.writeClassAndObject(output, spanKey);
            output.flush();
            persisted = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Input persistedInput = new Input(persisted);
        Kryo kryo2 = new Kryo();
        Object result = kryo2.readClassAndObject(persistedInput);

        verifySpanData((SpanKey) result, spanKey);
    }

    @Test
    public void testSerWithoutClassWritten() {
        UserKeyMetaValue metaValue = new UserKeyMetaValue();
        metaValue.setValue("a");
        Map<String, UserKeyMetaValue> metaValueMap = new HashMap<>();
        metaValueMap.put("b", metaValue);
        UserKey userKey1 = new UserKey();
        userKey1.setKeyId("c");
        userKey1.setMeta(metaValueMap);
        SpanKey<UserKey> spanKey = new SpanKey<>("d", "e", userKey1);

        byte[] persisted = null;

        // ser
        Kryo kryo1 = new Kryo();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Output output = new Output(baos);
            kryo1.writeObject(output, spanKey);
            output.flush();
            persisted = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //deser
        Input persistedInput = new Input(persisted);
        Kryo kryo2 = new Kryo();
        SpanKey result = kryo2.readObject(persistedInput, SpanKey.class);
        verifySpanData(result, spanKey);
    }

    @Test
    public void testSpanDataSerDserImplWithSimpleString() {
        SpanKey<String> stringSpanKey = new SpanKey<>("a", "b", "c");

        BaseSpanKeySerializer<String> ser = new BaseSpanKeySerializer<>();

        byte[] persisted = null;
        persisted = ser.serialize("x", stringSpanKey);

        BaseSpanKeySerializer<String> deser = new BaseSpanKeySerializer<>();

        SpanKey<String> result = deser.deserialize("y", persisted);

        verifySpanData(result, stringSpanKey);
    }

    @Test
    public void testSpanDataSerDserImplWithNestedType() {
        UserKeyMetaValue metaValue = new UserKeyMetaValue();
        metaValue.setValue("a");
        Map<String, UserKeyMetaValue> metaValueMap = new HashMap<>();
        metaValueMap.put("b", metaValue);
        UserKey userKey1 = new UserKey();
        userKey1.setKeyId("c");
        userKey1.setMeta(metaValueMap);
        SpanKey<UserKey> spanKey = new SpanKey<>("d", "e", userKey1);

        BaseSpanKeySerializer<UserKey> ser = new BaseSpanKeySerializer<>();
        ser.kryoRegister(UserKey.class);
        ser.kryoRegister(UserKeyMetaValue.class);
        ser.kryoRegister(HashMap.class);

        byte[] persisted = null;
        persisted = ser.serialize("x", spanKey);

        BaseSpanKeySerializer<UserKey> deser = new BaseSpanKeySerializer<>();
        deser.kryoRegister(UserKey.class);
        deser.kryoRegister(UserKeyMetaValue.class);
        deser.kryoRegister(HashMap.class);

        SpanKey<UserKey> result = deser.deserialize("y", persisted);

        verifySpanData(result, spanKey);
    }
}
