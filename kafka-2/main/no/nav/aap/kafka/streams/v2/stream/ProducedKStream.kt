package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Table
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.extension.skipTombstone
import no.nav.aap.kafka.streams.v2.processor.logProduced
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.state.KeyValueStore

internal fun <V> KStream<String, V>.produceToTable(
    table: Table<V>,
    logValues: Boolean,
): org.apache.kafka.streams.kstream.KTable<String, V & Any> = this
    .logProduced(table, logValues)
    .toTable(Named.`as`("${table.name}-to-table"), materialized(table.stateStoreName, table.source))
    .skipTombstone(table)

internal fun <V> KStream<String, V>.produceToTopic(
    topic: Topic<V>,
    named: String,
    logValues: Boolean,
) = this
    .logProduced(named = named, logValues = logValues)
    .to(topic.name, topic.produced(named))

internal fun <V> materialized(storeName: String, topic: Topic<V>): Materialized<String, V?, KeyValueStore<Bytes, ByteArray>> =
    Materialized.`as`<String, V, KeyValueStore<Bytes, ByteArray>>(storeName)
        .withKeySerde(topic.keySerde)
        .withValueSerde(topic.valueSerde)
