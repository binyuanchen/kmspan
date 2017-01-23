package org.kmspan.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Users use this annotation to annotate a method which takes
 * {@link org.apache.kafka.clients.consumer.ConsumerRecords records} as input. By doing that,
 * kmspan will process the span event messages before and after the call to annotated methods.
 * For example,
 * <p>
 * <pre>{@code
 * @Spaned public void processPolledMessages(ConsumerRecords records) {
 * // calculations based on each message
 * // persisting some states into data store for each message
 * ...
 * }
 * }</pre>
 * <p>
 * The expectation of kmspan to such annotated user methods is that the same thread is used
 * to do all calculations and processing on input
 * {@link org.apache.kafka.clients.consumer.ConsumerRecords records}.
 * <p>
 * This annotation helps to generate 'rough' span events. More details of 'rough' span events
 * can be found in wiki of the kmspan at <a href="https://github.com/binyuanchen/kmspan/wiki">here</a>.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Spaned {
}
