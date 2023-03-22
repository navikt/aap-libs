@file:Suppress("UNCHECKED_CAST")

package no.nav.aap.kafka.streams.v2.extension

import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.Table
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.processor.LogProduceTableProcessor
import no.nav.aap.kafka.streams.v2.processor.LogProduceTopicProcessor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Repartitioned
import org.apache.kafka.streams.state.KeyValueStore

internal fun <T : Any> KStream<String, T>.produceWithLogging(
    topic: Topic<T>,
    named: String,
) = this
    .addProcessor(LogProduceTopicProcessor("log-${named}", topic))
    .to(topic.name, topic.produced(named))

internal fun <L : Any, R : Any, LR> KStream<String, L>.leftJoin(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (String, L, R?) -> LR,
): KStream<String, LR> = this
    .leftJoin(
        right.internalKTable,
        joiner,
        left leftJoin right
    )

internal fun <L : Any, R : Any, LR> KStream<String, L>.leftJoin(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R?) -> LR,
): KStream<String, LR> = this
    .leftJoin(
        right.internalKTable,
        joiner,
        left leftJoin right
    )

internal fun <L : Any, R : Any, LR> KStream<String, L>.join(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (String, L, R) -> LR,
): KStream<String, LR> = this
    .join(
        right.tombstonedInternalKTable,
        joiner,
        left join right
    )

internal fun <L : Any, R : Any, LR> KStream<String, L>.join(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R) -> LR,
): KStream<String, LR> = this
    .join(
        right.tombstonedInternalKTable,
        joiner,
        left join right
    )

internal fun <T : Any> KStream<String, T?>.toKtable(table: Table<T>): KTable<T> {
    val tableNamed = Named.`as`("${table.sourceTopicName}-to-table")
    val internalKTable = this
        .addProcessor(LogProduceTableProcessor(table))
        .toTable(tableNamed, materialized(table))
    return KTable(table, internalKTable)
}

internal fun <T> repartitioned(table: Table<T & Any>, partitions: Int): Repartitioned<String, T> =
    Repartitioned
        .with(table.sourceTopic.keySerde, table.sourceTopic.valueSerde)
        .withNumberOfPartitions(partitions)
        .withName(table.sourceTopicName)

internal fun <T : Any> materialized(table: Table<T>): Materialized<String, T?, KeyValueStore<Bytes, ByteArray>> =
    Materialized.`as`<String, T, KeyValueStore<Bytes, ByteArray>>(table.stateStoreName)
        .withKeySerde(table.sourceTopic.keySerde)
        .withValueSerde(table.sourceTopic.valueSerde)

internal fun <V> KStream<String, V>.filterNotNull(): KStream<String, V & Any> {
    val filteredInternalKStream = filter { _, value -> value != null }
    return filteredInternalKStream as KStream<String, V & Any>
}

internal fun <T> org.apache.kafka.streams.kstream.KTable<String, T>.skipTombstone(
    table: Table<T & Any>
): org.apache.kafka.streams.kstream.KTable<String, T & Any> {
    val named = Named.`as`("skip-table-${table.sourceTopicName}-tombstone")
    val filteredInternalKTable = filter({ _, value -> value != null }, named)
    return filteredInternalKTable as org.apache.kafka.streams.kstream.KTable<String, T & Any>
}

internal fun <V> KStream<String, V>.skipTombstone(topic: Topic<V & Any>): KStream<String, V & Any> =
    skipTombstone(topic, "")

internal fun <V> KStream<String, V>.skipTombstone(
    topic: Topic<V & Any>,
    namedSuffix: String,
): KStream<String, V & Any> {
    val named = Named.`as`("skip-${topic.name}-tombstone$namedSuffix")
    val filteredInternalStream = filter({ _, value -> value != null }, named)
    return filteredInternalStream as KStream<String, V & Any>
}
