package no.nav.aap.kafka.streams.v2.extension

import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.processor.logConsumed
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream

fun <V> StreamsBuilder.consume(
    topic: Topic<V>,
    logValue: Boolean = false
): KStream<String, V?> = this
    .stream(topic.name, topic.consumed("consume-${topic.name}"))
    .logConsumed(topic, logValue)
