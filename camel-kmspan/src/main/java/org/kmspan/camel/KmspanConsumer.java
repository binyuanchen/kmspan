/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kmspan.camel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.util.IOHelper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.kmspan.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KmspanConsumer extends DefaultConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KmspanConsumer.class);

    protected ExecutorService executor;
    private final KmspanEndpoint endpoint;
    private final Processor processor;
    private final Long pollTimeoutMs;
    // This list helps working around the infinite loop of KAFKA-1894
    private final List<SpanAwareKafkaMessageFetcher> tasks = new ArrayList<>();

    //kmspan span event handlers
    private SpanEventListener spanEventListener;
    private SpanEventHandler spanEventHandler;
    private CuratorFramework curatorFramework;

    public KmspanConsumer(KmspanEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.processor = processor;
        this.pollTimeoutMs = endpoint.getConfiguration().getPollTimeoutMs();

        if (endpoint.getConfiguration().getBrokers() == null) {
            throw new IllegalArgumentException("BootStrap servers must be specified");
        }
        if (endpoint.getConfiguration().getGroupId() == null) {
            throw new IllegalArgumentException("groupId must not be null");
        }

        // checking span event handler parameters
        if (endpoint.getConfiguration().getSpanCoordZkServers() == null) {
            throw new IllegalArgumentException("Span event zk servers must be specified");
        }
        if (endpoint.getConfiguration().getSpanSCBeginZPath() == null) {
            throw new IllegalArgumentException("Span event sc begin zk path must be specified");
        }
        if (endpoint.getConfiguration().getSpanSCEndZPath() == null) {
            throw new IllegalArgumentException("Span event sc end zk path must be specified");
        }
        if (endpoint.getConfiguration().getConsumersCount() <= 0) {
            throw new IllegalArgumentException("consumer count must be greater than 0");
        }
    }

    Properties getProps() {
        Properties props = endpoint.getConfiguration().createConsumerProperties();
        endpoint.updateClassProperties(props);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint.getConfiguration().getBrokers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, endpoint.getConfiguration().getGroupId());
        return props;
    }

    @Override
    protected void doStart() throws Exception {
        LOG.info("Starting Kafka consumer");
        super.doStart();

        for (int i = 0; i < endpoint.getConfiguration().getConsumersCount(); i++) {
            tasks.add(
                    new SpanAwareKafkaMessageFetcher(endpoint.getConfiguration().getTopic(), i + "", getProps())
            );
        }

        // start zk client preparing for coordination of span events
        this.curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(endpoint.getConfiguration().getSpanCoordZkServers())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.curatorFramework.start();
        this.spanEventHandler = new KafkaZKSpanEventHandler(
                this.curatorFramework,
                this.tasks.get(0).getConsumer(), // just pick one, assuming they all work
                endpoint.getConfiguration().getSpanSCBeginZPath(),
                endpoint.getConfiguration().getSpanSCEndZPath()
        );
        Object spanEventListenerObj =
                endpoint.getCamelContext().getRegistry().lookupByName(KmspanConstants.SPAN_EVENT_LISTEN_REGISTRY_NAME);
        if (spanEventListenerObj == null) {
            LOG.warn("found no registry for {}", KmspanConstants.KAFKA_DEFAULT_DESERIALIZER);
        }
        else {
            this.spanEventListener = (SpanEventListener) spanEventListenerObj;
        }
        this.spanEventHandler.registerSpanEventListener(this.spanEventListener);

        executor = endpoint.createExecutor();
        for (SpanAwareKafkaMessageFetcher task: this.tasks) {
            executor.submit(task);
        }
    }

    @Override
    protected void doStop() throws Exception {
        LOG.info("Stopping Kafka consumer");

        if (executor != null) {
            if (getEndpoint() != null && getEndpoint().getCamelContext() != null) {
                getEndpoint().getCamelContext().getExecutorServiceManager().shutdownGraceful(executor);
            } else {
                executor.shutdownNow();
            }
            if (!executor.isTerminated()) {
                tasks.forEach(SpanAwareKafkaMessageFetcher::shutdown);
                executor.shutdownNow();
            }
        }
        tasks.clear();
        executor = null;

        curatorFramework.close();

        super.doStop();
    }

    class SpanAwareKafkaMessageFetcher implements Runnable {

        private final org.apache.kafka.clients.consumer.KafkaConsumer consumer;
        private final String topicName;
        private final String threadId;
        private final Properties kafkaProps;

        SpanAwareKafkaMessageFetcher(String topicName, String id, Properties kafkaProps) {
            this.topicName = topicName;
            this.threadId = topicName + "-" + "Thread " + id;
            this.kafkaProps = kafkaProps;
            
            ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                // Kafka uses reflection for loading authentication settings, use its classloader
                Thread.currentThread().setContextClassLoader(org.apache.kafka.clients.consumer.KafkaConsumer.class.getClassLoader());
                this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(kafkaProps);
            } finally {
                Thread.currentThread().setContextClassLoader(threadClassLoader);
            }
        }

        public KafkaConsumer getConsumer() {
            return consumer;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            try {
                LOG.info("Subscribing {} to topic {}", threadId, topicName);
                consumer.subscribe(Arrays.asList(topicName.split(",")));

                if (endpoint.getConfiguration().isSeekToBeginning()) {
                    LOG.debug("{} is seeking to the beginning on topic {}", threadId, topicName);
                    // This poll to ensures we have an assigned partition otherwise seek won't work
                    consumer.poll(100);
                    consumer.seekToBeginning(consumer.assignment());
                }
                while (isRunAllowed() && !isStoppingOrStopped() && !isSuspendingOrSuspended()) {
                    ConsumerRecords<Object, Object> allRecords = consumer.poll(pollTimeoutMs);
                    for (TopicPartition partition : allRecords.partitions()) {
                        List<ConsumerRecord<Object, Object>> partitionRecords = allRecords
                            .records(partition);
                        for (ConsumerRecord<Object, Object> record : partitionRecords) {
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("partition = {}, offset = {}, key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
                            }
                            // kmspan kicks in here to figure out carried span events
                            Object key = record.key();
                            if (key instanceof SpanData) {
                                SpanData spanKey = (SpanData) key;
                                if (spanKey.getSpanEventType() != null) {
                                    // TODO apparently this is still 'rough' event boundaries
                                    if (spanEventHandler != null) {
                                        spanEventHandler.handle(Arrays.asList(
                                                new ConsumerSpanEvent(
                                                        spanKey.getSpanId(),
                                                        spanKey.getSpanEventType(),
                                                        spanKey.getGenerationTimestamp(),
                                                        record.topic()
                                                )
                                        ));
                                    }
                                }
                                else {
                                    // this is a user message
                                    ConsumerRecord userMessage = new ConsumerRecord(
                                            topicName,
                                            partition.partition(),
                                            record.offset(),
                                            spanKey.getData(),
                                            record.value());
                                    // then send to user thread only the meat
                                    Exchange exchange = endpoint.createKafkaExchange(userMessage);
                                    try {
                                        processor.process(exchange);
                                    } catch (Exception e) {
                                        getExceptionHandler().handleException("Error during processing", exchange, e);
                                    }

                                }
                            }
                        }
                        // if autocommit is false
                        if (endpoint.getConfiguration().isAutoCommitEnable() != null
                            && !endpoint.getConfiguration().isAutoCommitEnable()) {
                            long partitionLastoffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                            consumer.commitSync(Collections.singletonMap(
                                partition, new OffsetAndMetadata(partitionLastoffset + 1)));
                        }
                    }
                }
                LOG.info("Unsubscribing {} from topic {}", threadId, topicName);
                consumer.unsubscribe();
            } catch (InterruptException e) {
                getExceptionHandler().handleException("Interrupted while consuming " + threadId + " from kafka topic", e);
                LOG.info("Unsubscribing {} from topic {}", threadId, topicName);
                consumer.unsubscribe();
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                getExceptionHandler().handleException("Error consuming " + threadId + " from kafka topic", e);
            } finally {
                LOG.debug("Closing {} ", threadId);
                IOHelper.close(consumer);
            }
        }

        private void shutdown() {
            // As advised in the KAFKA-1894 ticket, calling this wakeup method breaks the infinite loop
            consumer.wakeup();
        }
    }

}

