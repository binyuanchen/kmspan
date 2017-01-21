# kmspan

Welcome to kmspan!

A 'span' is a series of Kafka messages that are related to each other from an user's point of view. For example, when a user writes some code to read a file and transform each line of the file to a Kafka message and send them to Kafka, all messages in this file can be considered in the same span. Or the user may consider all messages transformed for all lines in several files to be in the same span, the definition of span is driven by business asks. At times, we may wonder, when is the end of a span? Naturally, Kafka producer is the role that knows the first message and the last message in the span. But knowing this usually does not help, as the question before usually means how to find out when the last message is processed. The 'process' here means user-specific/business-driven operations on the message such as doing calculation on the message and persisting results.

This is where kmspan can come to help.

Here are some basic terminologies that kmspan uses:

    *span BEGIN - an event that signifies the time right before the first message of a span is processed,
    *span END - an event that signifies the time right after the last message of a span is processed,
    *span boundary - the BEGIN and/or the END events of a span,
    *span interval - the elapse of time bounded by the START and END events of a span.

Typically the processing (user-specific, business-driven) of the messages is done by Kafka consumers, kmspan helps producers and consumers to collaborate and provides the span boundary on the consumer side.

