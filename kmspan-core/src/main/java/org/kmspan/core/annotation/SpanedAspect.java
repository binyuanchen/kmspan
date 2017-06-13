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

    @Before("execing() && sep()")
    public void preProcessSpaned() {
        SpanMessageHandler handler = SpanEventTLHolder.getSpanEventHandler();
        List<SpanMessage> events = SpanEventTLHolder.getSpanEvents();
        if (events != null && !events.isEmpty()) {
            List<SpanMessage> spanMessageSubList = new ArrayList<>();
            Iterator<SpanMessage> it = events.iterator();
            while (it.hasNext()) {
                SpanMessage spanMessage = it.next();
                String spanEventType = spanMessage.getSpanEventType();
                if (spanMessage != null && spanEventType.equals(SpanConstants.SPAN_BEGIN)) {
                    spanMessageSubList.add(spanMessage);
                    it.remove();
                }
            }

            logger.trace("preProcessSpaned: number of events = {}", spanMessageSubList.size());
            for (SpanMessage se : spanMessageSubList) {
                logger.trace("preProcessSpaned: event = {}", se.toString());
            }

            handler.handle(spanMessageSubList);
        }
    }

    @After("execing() && sep()")
    public void postProcessSpaned() {
        SpanMessageHandler handler = SpanEventTLHolder.getSpanEventHandler();
        List<SpanMessage> events = SpanEventTLHolder.getSpanEvents();
        if (events != null && !events.isEmpty()) {
            List<SpanMessage> spanMessageSubList = new ArrayList<>();
            Iterator<SpanMessage> it = events.iterator();
            while (it.hasNext()) {
                SpanMessage spanMessage = it.next();
                String spanEventType = spanMessage.getSpanEventType();
                if (spanMessage != null && spanEventType.equals(SpanConstants.SPAN_END)) {
                    spanMessageSubList.add(spanMessage);
                    it.remove();
                }
            }

            logger.trace("postProcessSpaned: number of events = {}", spanMessageSubList.size());
            for (SpanMessage se : spanMessageSubList) {
                logger.trace("postProcessSpaned: event = {}", se.toString());
            }

            handler.handle(spanMessageSubList);
        }
    }
}
