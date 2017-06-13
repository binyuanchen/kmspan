package org.kmspan.core.serialization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kmspan.core.SpanConstants;
import org.kmspan.core.SpanKey;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Create a single instance of (de-)serializer and run multiple threads workers, each one is a trying to
 * serialize and deserialize its own messages. The messages keys for each worker must be different to
 * see if conflicts can happen.
 */
public class SpanKeyKryoSerDserMultiThreadedTest {
    private static final int NUM_WORKERS = 100;
    private static Logger LOG = LogManager.getLogger(SpanKeyKryoSerDserMultiThreadedTest.class);
    private ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);

    private StringUserKeySerDeser serDeser = new StringUserKeySerDeser();

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterClass
    public void cleanup() {
        executorService.shutdownNow();
    }

    @Test
    public void testMultiThreadedSerializeAndDeserializer() throws InterruptedException, ExecutionException {
        List<Callable<Boolean>> workers = new ArrayList<>(NUM_WORKERS);
        for (int i = 0; i < NUM_WORKERS; i++) {
            Worker worker = new Worker("worker" + i + "-", serDeser);
            workers.add(worker);
        }
        List<Future<Boolean>> futures = executorService.invokeAll(workers);
        for (Future<Boolean> f : futures) {
            Assert.assertTrue(f.get());
        }
    }

    private static class StringUserKeySerDeser extends SpanDataSerDeser<String> {
    }

    private static class Worker implements Callable<Boolean> {
        private String userKeyPrefix;
        private StringUserKeySerDeser deser;

        public Worker(String userKeyPrefix, StringUserKeySerDeser deser) {
            this.userKeyPrefix = userKeyPrefix;
            this.deser = deser;
        }

        @Override
        public Boolean call() {
            String spanId = userKeyPrefix + "span";
            for (int i = 0; i < 10000; i++) {
                String userKey = userKeyPrefix + i;
                SpanKey<String> spanKey = new SpanKey<>(spanId, SpanConstants.SPAN_END, userKey);
                byte[] serializedSpanKey = deser.serialize("topic1", spanKey);
                SpanKey<String> deserializedSpanKey = deser.deserialize("topic1", serializedSpanKey);
                Assert.assertNotNull(deserializedSpanKey);
                Assert.assertEquals(deserializedSpanKey.getId(), spanId);
                Assert.assertNotNull(deserializedSpanKey.getData());
                Assert.assertEquals(deserializedSpanKey.getData(), userKey);
            }

            return true;
        }
    }
}
