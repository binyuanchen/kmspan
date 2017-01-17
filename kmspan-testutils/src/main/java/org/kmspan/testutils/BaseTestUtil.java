package org.kmspan.testutils;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class BaseTestUtil {
    private static Logger logger = LogManager.getLogger(BaseTestUtil.class);

    public static String generateRandomeDataDir() {
        return "data-" + UUID.randomUUID().toString();
    }

    public static String generateRandomTopicName() {
        return "TOPIC-" + UUID.randomUUID().toString();
    }

    public static String generateRandomSpanId() {
        return "SPAN-" + UUID.randomUUID().toString();
    }

    /**
     * Either with the test in-memory kafka broker or real kafka broker (2.11_0.10.1.0), if
     * the broker does a clean startup (kafka and zookeeper data are removed before startup),
     * an issue with this test is the KafkaConsumer.poll(timeout) always return empty. Now a
     * temporary workaround is to create a dummy consumer and stop is shortly after it was
     * started, discard it, then create another consumer for real testing.
     * TODO need to fix this somehow
     */
    public static void applyWorkaround(String kafkaRunningAddr) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaRunningAddr);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("topic_test"));
        logger.info("dieSoonConsumer will start polling");
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 2000L) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
            }
        }
        consumer.close();
    }

    public static void createTopic(
            String zkConnectStr, String topic, int numOfPatitions, int numOfReplications) {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;

        try {
            zkClient = new ZkClient(zkConnectStr, 15000, 10000, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnectStr), false);
            Properties properties = new Properties();
            AdminUtils.createTopic(zkUtils, topic, numOfPatitions, numOfReplications, properties, RackAwareMode.Enforced$.MODULE$);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create kafka topic", e);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public static void main(String[] args) {
        LocalZookeeperServer zk1, zk2, zk3;
        LocalKafkaBroker kf1, kf2, kf3;

        zk1 = new LocalZookeeperServer("localhost", 0);
        zk2 = new LocalZookeeperServer("localhost", 0);
        zk3 = new LocalZookeeperServer("localhost", 0);

        zk1.startupIfNot();
        zk2.startupIfNot();
        zk3.startupIfNot();

        kf1 = new LocalKafkaBroker(zk1.getRunningHost(), zk1.getRunningPort(), "localhost", 0);
        kf2 = new LocalKafkaBroker(zk2.getRunningHost(), zk2.getRunningPort(), "localhost", 0);
        kf3 = new LocalKafkaBroker(zk3.getRunningHost(), zk3.getRunningPort(), "localhost", 0);

        kf1.startupIfNot();
        kf2.startupIfNot();
        kf3.startupIfNot();

        try {
            Thread.sleep(180000L);
        } catch (InterruptedException e) {

        }

        kf1.shutdown();
        kf2.shutdown();
        kf3.shutdown();

        zk1.shutdown();
        zk2.shutdown();
        zk3.shutdown();
    }
}
