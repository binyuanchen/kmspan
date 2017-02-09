package org.kmspan.core.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.SpanData;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

public class SpanDataKryoSerDserGeneralTest {
    private static Logger logger = LogManager.getLogger(SpanDataKryoSerDserGeneralTest.class);

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterClass
    public void cleanup() {
    }

    private void verifySpanData(SpanData actual, SpanData expected) {
        Assert.assertNotNull(actual);
        Assert.assertNotNull(expected);
        Assert.assertEquals(actual.getSpanId(), expected.getSpanId());
        Assert.assertEquals(actual.getSpanEventType(), expected.getSpanEventType());
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
        SpanData<UserKey> spanData = new SpanData<>("d", "e", userKey1);

        byte[] persisted = null;
        Kryo kryo1 = new Kryo();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Output output = new Output(baos);
            kryo1.writeClassAndObject(output, spanData);
            output.flush();
            persisted = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Input persistedInput = new Input(persisted);
        Kryo kryo2 = new Kryo();
        Object result = kryo2.readClassAndObject(persistedInput);

        verifySpanData((SpanData) result, spanData);
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
        SpanData<UserKey> spanData = new SpanData<>("d", "e", userKey1);

        byte[] persisted = null;

        // ser
        Kryo kryo1 = new Kryo();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Output output = new Output(baos);
            kryo1.writeObject(output, spanData);
            output.flush();
            persisted = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //deser
        Input persistedInput = new Input(persisted);
        Kryo kryo2 = new Kryo();
        SpanData result = kryo2.readObject(persistedInput, SpanData.class);
        verifySpanData(result, spanData);
    }

    @Test
    public void testSpanDataSerDserImplWithSimpleString() {
        SpanData<String> stringSpanData = new SpanData<>("a", "b", "c");

        SpanDataSerDeser<String> ser = new SpanDataSerDeser<>();

        byte[] persisted = null;
        persisted = ser.serialize("x", stringSpanData);

        SpanDataSerDeser<String> deser = new SpanDataSerDeser<>();

        SpanData<String> result = deser.deserialize("y", persisted);

        verifySpanData(result, stringSpanData);
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
        SpanData<UserKey> spanData = new SpanData<>("d", "e", userKey1);

        SpanDataSerDeser<UserKey> ser = new SpanDataSerDeser<>();
        ser.kryoRegister(UserKey.class);
        ser.kryoRegister(UserKeyMetaValue.class);
        ser.kryoRegister(HashMap.class);

        byte[] persisted = null;
        persisted = ser.serialize("x", spanData);

        SpanDataSerDeser<UserKey> deser = new SpanDataSerDeser<>();
        deser.kryoRegister(UserKey.class);
        deser.kryoRegister(UserKeyMetaValue.class);
        deser.kryoRegister(HashMap.class);

        SpanData<UserKey> result = deser.deserialize("y", persisted);

        verifySpanData(result, spanData);
    }
}
