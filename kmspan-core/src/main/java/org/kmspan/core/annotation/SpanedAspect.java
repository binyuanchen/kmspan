package org.kmspan.core.annotation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.kmspan.core.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Aspect
public class SpanedAspect {

    private static Logger logger = LogManager.getLogger(SpanedAspect.class);

    @Pointcut("@annotation(org.kmspan.core.annotation.Spaned)")
    public void sep() {
    }

    @Pointcut("execution(* *(..))")
    public void execing() {
    }

    /**
     * TODO
     * clearly there is a hole below, although it may not be a big deal for a span that has many
     * regular messages in between (thousands). Improve this if possible.
     *
     * Next step: at lease guarantee the relative ordering of span events among span. (using timestamp
     * of events or its kafka offset for ordering?)
     *
     * Next next step: can we further guarantee the relative ordering among span events and regular messages?
     * (a new poll api with iterator returned?)
     */

    @Before("execing() && sep()")
    public void preProcessSpaned() {
        SpanEventHandler handler = SpanEventTLHolder.getSpanEventHandler();
        List<ConsumerSpanEvent> events = SpanEventTLHolder.getSpanEvents();
        if (events != null && !events.isEmpty()) {
            List<ConsumerSpanEvent> consumerSpanEventSubList = new ArrayList<>();
            Iterator<ConsumerSpanEvent> it = events.iterator();
            while (it.hasNext()) {
                ConsumerSpanEvent consumerSpanEvent = it.next();
                String spanEventType = consumerSpanEvent.getSpanEventType();
                if (consumerSpanEvent != null && spanEventType.equals(SpanConstants.SPAN_BEGIN)) {
                    consumerSpanEventSubList.add(consumerSpanEvent);
                    it.remove();
                }
            }

            logger.trace("preProcessSpaned: number of events = {}", consumerSpanEventSubList.size());
            for (ConsumerSpanEvent se : consumerSpanEventSubList) {
                logger.trace("preProcessSpaned: event = {}", se.toString());
            }

            handler.handle(consumerSpanEventSubList);
        }
    }

    @After("execing() && sep()")
    public void postProcessSpaned() {
        SpanEventHandler handler = SpanEventTLHolder.getSpanEventHandler();
        List<ConsumerSpanEvent> events = SpanEventTLHolder.getSpanEvents();
        if (events != null && !events.isEmpty()) {
            List<ConsumerSpanEvent> consumerSpanEventSubList = new ArrayList<>();
            Iterator<ConsumerSpanEvent> it = events.iterator();
            while (it.hasNext()) {
                ConsumerSpanEvent consumerSpanEvent = it.next();
                String spanEventType = consumerSpanEvent.getSpanEventType();
                if (consumerSpanEvent != null && spanEventType.equals(SpanConstants.SPAN_END)) {
                    consumerSpanEventSubList.add(consumerSpanEvent);
                    it.remove();
                }
            }

            logger.trace("postProcessSpaned: number of events = {}", consumerSpanEventSubList.size());
            for (ConsumerSpanEvent se : consumerSpanEventSubList) {
                logger.trace("postProcessSpaned: event = {}", se.toString());
            }

            handler.handle(consumerSpanEventSubList);
        }
    }
}
