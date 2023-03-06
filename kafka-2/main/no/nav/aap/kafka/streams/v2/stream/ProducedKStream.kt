package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.processor.LogProduceTopicProcessor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore

internal fun <T : Any> KStream<String, T>.produceToTopic(
    topic: Topic<T>,
    named: String,
    logValues: Boolean,
) = this
    .addProcessor(LogProduceTopicProcessor("log-${named}", logValues))
    .to(topic.name, topic.produced(named))

internal fun <T : Any> materialized(
    storeName: String,
    topic: Topic<T>
): Materialized<String, T?, KeyValueStore<Bytes, ByteArray>> =
    Materialized.`as`<String, T, KeyValueStore<Bytes, ByteArray>>(storeName)
        .withKeySerde(topic.keySerde)
        .withValueSerde(topic.valueSerde)
