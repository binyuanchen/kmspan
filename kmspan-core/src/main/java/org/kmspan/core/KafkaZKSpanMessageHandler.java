package org.kmspan.core;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link SpanMessageHandler SpanMessageHandler} wherein handlers collaborate using Zookeeper.
 */
public class KafkaZKSpanMessageHandler implements SpanMessageHandler {
    private static Logger logger = LogManager.getLogger(KafkaZKSpanMessageHandler.class);

    private CuratorFramework curatorFramework;
    // TODO remove this and uses scTargetCount instead, this class should only depends on
    // Zookeeper, not Kafka
    private KafkaConsumer kafkaConsumer;
    private String spanBeginSCZPath;
    private String spanEndSCZPath;
    // used preferably to kafkaConsumer if set
    private int scTargetCount = -1;

    private List<SpanEventListener> spanEventListeners = new ArrayList<>();

    public KafkaZKSpanMessageHandler(CuratorFramework curatorFramework,
                                     int scTargetCount,
                                     String spanBeginSCZPath,
                                     String spanEndSCZPath) {
        this(curatorFramework, scTargetCount, spanBeginSCZPath, spanEndSCZPath, new ArrayList<>());
    }

    public KafkaZKSpanMessageHandler(CuratorFramework curatorFramework,
                                     int scTargetCount,
                                     String spanBeginSCZPath,
                                     String spanEndSCZPath,
                                     List<SpanEventListener> spanEventListeners) {
        this.curatorFramework = curatorFramework;
        this.scTargetCount = scTargetCount;
        this.spanBeginSCZPath = spanBeginSCZPath;
        this.spanEndSCZPath = spanEndSCZPath;
        this.spanEventListeners = spanEventListeners;
    }

    public KafkaZKSpanMessageHandler(CuratorFramework curatorFramework,
                                     KafkaConsumer kafkaConsumer,
                                     String spanBeginSCZPath,
                                     String spanEndSCZPath) {
        this(curatorFramework, kafkaConsumer, spanBeginSCZPath, spanEndSCZPath, new ArrayList<>());
    }

    public KafkaZKSpanMessageHandler(CuratorFramework curatorFramework,
                                     KafkaConsumer kafkaConsumer,
                                     String spanBeginSCZPath,
                                     String spanEndSCZPath,
                                     List<SpanEventListener> spanEventListeners) {
        this.curatorFramework = curatorFramework;
        this.kafkaConsumer = kafkaConsumer;
        this.spanBeginSCZPath = spanBeginSCZPath;
        this.spanEndSCZPath = spanEndSCZPath;
        this.spanEventListeners = spanEventListeners;
    }

    @Override
    public void registerSpanEventListener(SpanEventListener listener) {
        this.spanEventListeners.add(listener);
    }

    @Override
    public void handle(List<ConsumerSpanEvent> consumerSpanEvents) {
        for (ConsumerSpanEvent consumerSpanEvent : consumerSpanEvents) {
            String spanId = consumerSpanEvent.getSpanId();
            String spanEventType = consumerSpanEvent.getSpanEventType();
            String topic = consumerSpanEvent.getTopic();
            // TODO make 2 cases, async and sync
            if (spanEventType.equals(SpanConstants.SPAN_BEGIN)) {
                int targetCount;
                if (this.scTargetCount <= 0) {
                    List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
                    targetCount = partitionInfoList.size();
                } else {
                    targetCount = this.scTargetCount;
                }
                final String zpath = spanBeginSCZPath + "/" + spanId;
                SharedCount sharedCount = new SharedCount(curatorFramework, zpath, 0);
                logger.debug("[{}]created sharedCount, znode={}",
                        SpanConstants.SPAN_BEGIN, zpath);
                try {
                    sharedCount.start();
                    logger.debug("[{}]started sharedCount, znode={}",
                            SpanConstants.SPAN_BEGIN, zpath);
                    boolean success = false;
                    int retry = 0;
                    while (!success && retry++ < 2 * targetCount) {
                        VersionedValue<Integer> current = sharedCount.getVersionedValue();
                        int newCount = current.getValue() + 1;
                        success = sharedCount.trySetCount(current, newCount);
                    }
                    if (retry >= 2 * targetCount) {
                        throw new Exception("[" + SpanConstants.SPAN_BEGIN + "]failed to set sc " + zpath + " after " + 2 * targetCount + " retries");
                    }
                    int collected = sharedCount.getCount();
                    if (collected == 1) {
                        // this is the begin of span
                        logger.debug("[{}][begin of span {}]",
                                SpanConstants.SPAN_BEGIN, spanId);
                        // amplify this event
                        for (SpanEventListener listener : this.spanEventListeners) {
                            listener.onSpanEvent(
                                    ConsumerSpanEvent.createSpanEvent(
                                            consumerSpanEvent.getSpanId(),
                                            consumerSpanEvent.getSpanEventType(),
                                            consumerSpanEvent.getTopic())
                                    );
                        }
                    }
                    // no 'else if', in this way, we also deal with the scenario when numberOfPartitions==1
                    if (collected == targetCount) {
                        logger.debug("[{}]collected={} matches targetCount={}",
                                SpanConstants.SPAN_BEGIN, collected, targetCount);
                        sharedCount.close();
                        logger.debug("[{}]closed sharedCount at znode={}",
                                SpanConstants.SPAN_BEGIN, zpath);
                        sharedCount = null;
                        curatorFramework.delete().forPath(zpath);
                        logger.debug("[{}]deleted znode {}",
                                SpanConstants.SPAN_BEGIN, zpath);
                    } else if (collected > targetCount) {
                        // in this case, do not delete znode for troubleshooting, TODO async delete
                        logger.error("[{}]error, collected={} is greater than partitions={}",
                                SpanConstants.SPAN_BEGIN, collected, targetCount);
                    }
                } catch (Exception e) {
                    logger.error("", e);
                } finally {
                    try {
                        if (sharedCount != null) {
                            sharedCount.close();
                        }
                    } catch (IOException e1) {
                        logger.error("", e1);
                    }
                }
            } else if (spanEventType.equals(SpanConstants.SPAN_END)) {
                int targetCount;
                if (this.scTargetCount <= 0) {
                    List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);
                    targetCount = partitionInfoList.size();
                } else {
                    targetCount = this.scTargetCount;
                }
                final String zpath = spanEndSCZPath + "/" + spanId;
                SharedCount sharedCount = new SharedCount(curatorFramework, zpath, 0);
                logger.debug("[{}]created sharedCount, znode={}",
                        SpanConstants.SPAN_END, zpath);
                try {
                    sharedCount.start();
                    logger.debug("[{}]started sharedCount, znode={}",
                            SpanConstants.SPAN_END, zpath);
                    boolean success = false;
                    int retry = 0;
                    while (!success && retry++ < 2 * targetCount) {
                        VersionedValue<Integer> current = sharedCount.getVersionedValue();
                        int newCount = current.getValue() + 1;
                        success = sharedCount.trySetCount(current, newCount);
                    }
                    if (retry >= 2 * targetCount) {
                        throw new Exception("[" + SpanConstants.SPAN_END + "]failed to set sc " + zpath + " after " + 2 * targetCount + " retries");
                    }
                    int collected = sharedCount.getCount();
                    if (collected == targetCount) {
                        logger.debug("[{}]collected={} matches targetCount={}",
                                SpanConstants.SPAN_END, collected, targetCount);
                        sharedCount.close();
                        logger.debug("[{}]closed sharedCount at znode={}",
                                SpanConstants.SPAN_END, zpath);
                        sharedCount = null;
                        curatorFramework.delete().forPath(zpath);
                        // this is the end of a span
                        logger.debug("[{}]deleted znode {}",
                                SpanConstants.SPAN_END, zpath);
                        logger.debug("[{}][end of span {}]",
                                SpanConstants.SPAN_END, spanId);
                        for (SpanEventListener listener : this.spanEventListeners) {
                            listener.onSpanEvent(
                                    ConsumerSpanEvent.createSpanEvent(
                                            consumerSpanEvent.getSpanId(),
                                            consumerSpanEvent.getSpanEventType(),
                                            consumerSpanEvent.getTopic()
                                    )
                            );
                        }
                    } else if (collected > targetCount) {
                        // in this case, do not delete that node for debugging, TODO async if delete
                        logger.debug("[{}]error, collected={} is greater than partitions={}",
                                SpanConstants.SPAN_END, collected, targetCount);
                    }
                } catch (Exception e) {
                    logger.error("", e);
                } finally {
                    try {
                        if (sharedCount != null) {
                            sharedCount.close();
                        }
                    } catch (IOException e1) {
                        logger.error("", e1);
                    }
                }
            }
        }

    }
}
