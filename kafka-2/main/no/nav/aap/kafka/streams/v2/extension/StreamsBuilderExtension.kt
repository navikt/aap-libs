package no.nav.aap.kafka.streams.v2.extension

import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.LogConsumeTopicProcessor
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream

internal fun <T> StreamsBuilder.consume(
    topic: Topic<T>,
    logValue: Boolean = false //TODO: ingen default her
): KStream<String, T?> = this
    .stream(topic.name, topic.consumed("consume-${topic.name}"))
    .addProcessor(
        LogConsumeTopicProcessor(
            named = "log-consume-${topic.name}",
            logValue = logValue
        )
    )
