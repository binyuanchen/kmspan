package org.kmspan.testutils;

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.RunningAsBroker;
import kafka.utils.SystemTime$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Option;
import scala.collection.mutable.ArraySeq;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * How to see the logs of this test kafka broker when running from
 * {@link LocalKafkaBroker#main(String[])} main? Configure VM Options:
 * <p>
 * -Dkafka.logs.dir=/tmp/kafka_logs1 -Dlog4j.configuration=file:/path/to/official_kafka_log4j.properties
 */
public class LocalKafkaBroker {
    private static Logger logger = LogManager.getLogger(LocalKafkaBroker.class);
    private static KafkaServer kafkaServer;
    public String runningHost = "localhost";
    private volatile int runningPort = 9092;
    private String zookeeperHost = "localhost";
    private int zookeeperPort = 2181;
    private File dataDir = null;

    public LocalKafkaBroker(String zookeeperHost, int zookeeperPort) {
        this(zookeeperHost, zookeeperPort, "localhost", 9092);
    }

    public LocalKafkaBroker(String zookeeperHost, int zookeeperPort, int brokerId) {
        this(zookeeperHost, zookeeperPort, "localhost", 9092);
    }

    public LocalKafkaBroker(String zookeeperHost, int zookeeperPort, File dataDir) {
        this(zookeeperHost, zookeeperPort, "localhost", 9092, dataDir);
    }

    public LocalKafkaBroker(String zookeeperHost,
                            int zookeeperPort,
                            String runningHost,
                            int runningPort) {
        this(zookeeperHost, zookeeperPort, runningHost, runningPort,
                new File(System.getProperty("java.io.tmpdir"), BaseTestUtil.generateRandomeDataDir()));
    }

    public LocalKafkaBroker(String zookeeperHost,
                            int zookeeperPort,
                            String runningHost,
                            int runningPort,
                            File dataDir) {
        if (zookeeperHost == null || zookeeperPort < 0
                || runningHost == null || runningPort < 0 || dataDir == null) {
            throw new IllegalArgumentException();
        }
        this.zookeeperHost = zookeeperHost;
        this.zookeeperPort = zookeeperPort;
        this.runningHost = runningHost;
        this.runningPort = runningPort;
        this.dataDir = dataDir;
    }


    public static void main(String[] args) throws Exception {
        LocalKafkaBroker server = new LocalKafkaBroker("localhost", 2181, new File("/tmp/kmspan_kafka"));
        server.startupIfNot();
//        server.shutdown();
    }

    private String getZookeeperAddr() {
        return zookeeperHost + ":" + zookeeperPort;
    }

    public String getRunningHost() {
        return runningHost;
    }

    public String getRunningAddr() {
        return runningHost + ":" + runningPort;
    }

    public synchronized void startupIfNot() {
        if (kafkaServer == null) {
            logger.info("[TEST KAFKA BROKER] is using data dir {}",
                    this.dataDir.getAbsolutePath());

            Properties properties = new Properties();
            properties.put("broker.id", 0);
            properties.put("num.network.threads", 3);
            properties.put("num.io.threads", 4);
            properties.put("socket.send.buffer.bytes", 16384);
            properties.put("socket.receive.buffer.bytes", 16384);
            properties.put("socket.request.max.bytes", 10485760);
            properties.put("log.dirs", this.dataDir.getAbsolutePath());
            properties.put("num.partitions", 1);
            properties.put("num.recovery.threads.per.data.dir", 1);
            properties.put("log.retention.hours", 2);
            properties.put("log.segment.bytes", 33554432);
            properties.put("log.retention.check.interval.ms", 300000);
            properties.put("zookeeper.connect", getZookeeperAddr());
            properties.put("zookeeper.connection.timeout.ms", 5000);

            properties.put("listeners", "PLAINTEXT://" + runningHost + ":" + runningPort);
            //properties.put("advertised.listeners", "PLAINTEXT://" + RUNNING_HOST + ":0");
            // must explicitly create topic before using it, this forces tests to
            // explicitly create topic they need.
            properties.put("auto.create.topics.enable", false);
            // Optional, as each test suite run will start new in-memory kafka and
            // zookeeper servers, which will delete their data.
            properties.put("delete.topic.enable", true);

            kafkaServer = new KafkaServer(
                    KafkaConfig.fromProps(properties),
                    SystemTime$.MODULE$,
                    Option.apply("TestKafkaBrokerThread"),
                    new ArraySeq<KafkaMetricsReporter>(1));
            logger.info("[TEST KAFKA BROKER] starting");
            kafkaServer.startup();
            while (kafkaServer.brokerState().currentState() != RunningAsBroker.state()) {
                logger.info("[TEST KAFKA BROKER] not up yet");
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            //TODO if break point and freeze here, kafka can not register with zookeeper
            // correctly, check zpath /brokers/ids/ being empty
            ZkClient zkClient = ZkUtils.createZkClient(getZookeeperAddr(), 15000, 10000);
            List<String> children = zkClient.getChildren(ZkUtils.BrokerIdsPath());
            logger.info("[TEST KAFKA BROKER] brokers in zk = {}", children.toString());

            // refresh the running port (as it is a random one)
            this.runningPort = kafkaServer.boundPort(SecurityProtocol.PLAINTEXT);
            logger.info("[TEST KAFKA BROKER] up and running at host = {}, port = {}", runningHost, runningPort);
        }
    }

    public void shutdown() {
        if (kafkaServer != null) {
            try {
                logger.info("[TEST KAFKA BROKER] will stop");
                kafkaServer.shutdown();
                kafkaServer.awaitShutdown();
            } finally {
                try {
                    logger.info("[TEST KAFKA BROKER] will remove data dir {}", this.dataDir.getAbsolutePath());
                    FileUtils.deleteDirectory(this.dataDir);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to delete test kafka broker data dir "
                            + this.dataDir.getAbsolutePath(), e);
                }
            }
        }
    }
}
