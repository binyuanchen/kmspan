package org.kmspan.testutils;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

public class LocalZookeeperServer {

    private static Logger logger = LogManager.getLogger(LocalZookeeperServer.class);
    private File dataDir = null;
    private String runningHost = "localhost";
    private volatile int runningPort = 2181;
    private ServerCnxnFactory factory;

    public LocalZookeeperServer() {
        this("localhost", 2181);
    }

    public LocalZookeeperServer(File dataDir) {
        this("localhost", 2181, dataDir);
    }

    public LocalZookeeperServer(String runningHost, int runningPort) {
        this(runningHost, runningPort, new File(
                System.getProperty("java.io.tmpdir"),
                BaseTestUtil.generateRandomeDataDir()));
    }

    public LocalZookeeperServer(String runningHost, int runningPort, File dataDir) {
        if (runningHost == null || runningPort < 0 || dataDir == null) {
            throw new IllegalArgumentException();
        }
        this.runningHost = runningHost;
        this.runningPort = runningPort;
        this.dataDir = dataDir;
    }

    public static void main(String[] args) throws Exception {
        LocalZookeeperServer server = new LocalZookeeperServer(new File("/tmp/kmspan_zk"));
        server.startupIfNot();
//        server.shutdown();
    }

    public String getRunningHost() {
        return runningHost;
    }

    public int getRunningPort() {
        return runningPort;
    }

    public String getRunningAddr() {
        return runningHost + ":" + runningPort;
    }

    public synchronized void startupIfNot() {
        if (factory == null) {
            try {
                logger.info("[TEST ZOOKEEPER SERVER] is using data dir {}",
                        this.dataDir.getAbsolutePath());

                ZooKeeperServer server = new ZooKeeperServer(this.dataDir, this.dataDir, 2000);
                factory = ServerCnxnFactory.createFactory(
                        new InetSocketAddress(runningHost, runningPort), 1000);
                // refresh the port as it is a random one
                runningPort = factory.getLocalPort();
                String host = factory.getLocalAddress().getHostName();
                logger.info("[TEST ZOOKEEPER SERVER] starting, host = {}, port = {}",
                        host, runningPort);
                factory.startup(server); // async
                while (!server.isRunning()) {
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                    }
                }
                logger.info("[TEST ZOOKEEPER SERVER] up and running, host = {}, port = {}",
                        host, runningPort);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void shutdown() {
        if (factory != null) {
            try {
                logger.info("[TEST ZOOKEEPER SERVER] shutting down.");
                factory.shutdown();
            } finally {
                try {
                    logger.info("[TEST ZOOKEEPER SERVER] will remove data dir {}",
                            this.dataDir.getAbsolutePath());
                    FileUtils.deleteDirectory(this.dataDir);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to delete test zk server data dir "
                            + this.dataDir.getAbsolutePath(), e);
                }
            }
        }
    }
}

