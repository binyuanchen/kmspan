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
 * This test tests the initialization of ThreadLocal Kyro instance behavior when there are
 * high number of concurrent threads, and long list of classes to be registered to the
 * Kyro instance.
 */
public class KyroInstanceInitializationMultiThreadTest {
    private static final int NUM_WORKERS = 1200;
    // list of custom classes to register
    private final static List<Class> classesToRegister = new ArrayList<Class>() {{
        add(CustomClass1.class);
        add(CustomClass2.class);
        add(CustomClass3.class);
        add(CustomClass4.class);
        add(CustomClass5.class);
        add(CustomClass6.class);
        add(CustomClass7.class);
        add(CustomClass8.class);
        add(CustomClass9.class);
        add(CustomClass10.class);
    }};
    private static Logger LOG = LogManager.getLogger(KyroInstanceInitializationMultiThreadTest.class);
    private ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterClass
    public void cleanup() {
        executorService.shutdownNow();
    }

    @Test
    public void testTLSerializerInit() throws InterruptedException, ExecutionException {
        List<Callable<Boolean>> workers = new ArrayList<>(NUM_WORKERS);
        for (int i = 0; i < NUM_WORKERS; i++) {
            Worker worker = new Worker(i);
            workers.add(worker);
        }
        List<Future<Boolean>> futures = executorService.invokeAll(workers);
        for (Future<Boolean> f : futures) {
            Assert.assertTrue(f.get());
        }
    }

    // custom classes
    private static class CustomClass1 {
        // no need to implement hash in this test, it defaults to the identity of the object
    }

    private static class CustomClass2 {
    }

    private static class CustomClass3 {
    }

    private static class CustomClass4 {
    }

    private static class CustomClass5 {
    }

    private static class CustomClass6 {
    }

    private static class CustomClass7 {
    }

    private static class CustomClass8 {
    }

    private static class CustomClass9 {
    }

    private static class CustomClass10 {
    }

    private static class Worker implements Callable<Boolean> {
        private int i; // id of a worker
        private BaseSpanKeySerializer serializer = null;

        public Worker(int i) {
            this.i = i;
            // initialize in the main thread
            this.serializer = new BaseSpanKeySerializer();
        }

        @Override
        public Boolean call() {
            this.serializer.kryoRegisters(classesToRegister);
            String userKey = "userKey" + i;
            SpanKey<String> spanKey = new SpanKey<>("span" + i, SpanConstants.SPAN_END, userKey);
            this.serializer.serialize("topic1", spanKey);
            return true;
        }
    }
}
