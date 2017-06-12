package org.kmspan.camel;

import java.util.*;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultProducer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.kmspan.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KmspanProducer extends DefaultProducer {
    private static final Logger logger = LoggerFactory.getLogger(KmspanProducer.class);

    private final KmspanEndpoint endpoint;

    // listeners that registered interests in span events, now there can be only
    // one listener that comes as a camel registry
    private SpanEventListener spanEventListener;
    // handler that can handle span events
    private SpanEventHandler spanEventHandler;
    // handler uses this zookeeper client, now it is curator
    private CuratorFramework curatorFramework;
    // SC target count, must be the same as topic partition count.
    private int scTargetCount;

    public KmspanProducer(KmspanEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;

        if (endpoint.getConfiguration().getModeOrDefault() == null) {
            throw new IllegalArgumentException("modeOrDefault must be specified");
            // although we are not using it now
        }
        if (endpoint.getConfiguration().getSourceType() == null) {
            throw new IllegalArgumentException("sourceType must be specified");
            // although we are not using it now
        }
        if (endpoint.getConfiguration().getStorageType() == null) {
            throw new IllegalArgumentException("storageType must be specified");
            // although we are not using it now
        }

        if (endpoint.getConfiguration().getSpanZKQuorum() == null) {
            throw new IllegalArgumentException("spanZKQuorum must be specified");
        }
        if (endpoint.getConfiguration().getSpanSCBeginZPath() == null) {
            throw new IllegalArgumentException("spanSCBeginZPath must be specified");
        }
        if (endpoint.getConfiguration().getSpanSCEndZPath() == null) {
            throw new IllegalArgumentException("spanSCEndZPath must be specified");
        }
        if (endpoint.getConfiguration().getSpanSCTargetCount() == null) {
            throw new IllegalArgumentException("spanSCTargetCount must be specified");
        } else {
            this.scTargetCount =
                    Integer.parseInt(endpoint.getConfiguration().getSpanSCTargetCount());
            if (this.scTargetCount <= 0) {
                throw new IllegalArgumentException("scTargetCount must be positive");
            }
        }
    }


    @Override
    protected void doStart() throws Exception {
        logger.info("Starting Kmspan producer");
        // start zk client preparing for coordination of span events
        this.curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(endpoint.getConfiguration().getSpanZKQuorum())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.curatorFramework.start();
        this.spanEventHandler = new KafkaZKSpanEventHandler(
                this.curatorFramework,
                this.scTargetCount,
                endpoint.getConfiguration().getSpanSCBeginZPath(),
                endpoint.getConfiguration().getSpanSCEndZPath()
        );
        Object selObj = endpoint
                .getCamelContext()
                .getRegistry()
                .lookupByName(KmspanConstants.SPAN_EVENT_LISTENER_REGISTRY_NAME);
        if (selObj == null) {
            logger.warn("found no registry for {}", KmspanConstants.SPAN_EVENT_LISTENER_REGISTRY_NAME);
        } else {
            this.spanEventListener = (SpanEventListener) selObj;
        }
        this.spanEventHandler.registerSpanEventListener(this.spanEventListener);
    }

    @Override
    protected void doStop() throws Exception {
        logger.info("Stopping Kmspan producer");
        this.curatorFramework.close();
    }

    // right now assuming the source is camel-kafka, whose message is (SpanKey<K>, V)
    @Override
    @SuppressWarnings("unchecked")
    public void process(Exchange exchange) throws Exception {
        if (exchange.getIn() != null) {
            Message message = exchange.getIn();
            Long offset = (Long) message.getHeader(KafkaConstants.OFFSET);
            Integer partitionId = (Integer) message.getHeader(KafkaConstants.PARTITION);
            String topic = (String) message.getHeader(KafkaConstants.TOPIC);
            Object keyObj = message.getHeader(KafkaConstants.KEY);
            if (keyObj == null) {
                throw new Exception("No header " + KafkaConstants.KEY + " in exchange");
            }
            if (!(keyObj instanceof SpanKey)) {
                throw new Exception("key object is of wrong type " + keyObj.getClass());
            }
            SpanKey spanKey = (SpanKey) keyObj;

            if (spanKey.getType() != null) {
                if (spanEventHandler != null) {
                    spanEventHandler.handle(Arrays.asList(
                            ConsumerSpanEvent.createSpanEvent(
                                    spanKey.getId(),
                                    spanKey.getType(),
                                    topic
                            )
                    ));
                }
                // stop propagation of exchange
                exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            } else {
                exchange.getIn().setHeader(KafkaConstants.KEY, spanKey.getData());

                // TODO what if there is Out?
            }
        }
    }
}
