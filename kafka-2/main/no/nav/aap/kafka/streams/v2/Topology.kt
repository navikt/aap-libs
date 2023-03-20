package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.concurrency.Bufferable
import no.nav.aap.kafka.streams.v2.concurrency.RaceConditionBuffer
import no.nav.aap.kafka.streams.v2.extension.skipTombstone
import no.nav.aap.kafka.streams.v2.processor.LogConsumeTopicProcessor
import no.nav.aap.kafka.streams.v2.processor.LogProduceTableProcessor
import no.nav.aap.kafka.streams.v2.processor.MetadataProcessor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.ProcessorMetadata
import no.nav.aap.kafka.streams.v2.stream.ConsumedStream
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Repartitioned
import org.apache.kafka.streams.state.KeyValueStore

class Topology internal constructor() {
    private val builder = StreamsBuilder()

    fun <T : Any> consume(topic: Topic<T>): ConsumedStream<T> {
        val consumed = consumeWithLogging(topic).skipTombstone(topic)
        return ConsumedStream(topic, consumed) { "from-${topic.name}" }
    }

    fun <T : Any> consume(table: Table<T>): KTable<T> {
        val stream = consumeWithLogging(table.sourceTopic)
        return stream.toKtable(table)
    }

    private fun <T : Any> KStream<String, T?>.toKtable(table: Table<T>): KTable<T> {
        val tableNamed = Named.`as`("${table.sourceTopicName}-to-table")
        val materializedKeyValueStore = materialized(table.stateStoreName, table.sourceTopic)
        val internalKTable = this
            .addProcessor(LogProduceTableProcessor(table))
            .toTable(tableNamed, materializedKeyValueStore)
        return KTable(table, internalKTable)
    }

    fun <T : Bufferable<T>> consume(
        table: Table<T>,
        buffer: RaceConditionBuffer<T>,
    ): KTable<T> {
        val stream = consumeWithLogging(table.sourceTopic)
        stream.filter { _, value -> value == null }.foreach { key, _ -> buffer.slett(key) }
        return stream.toKtable(table)
    }

    fun <T : Any> consumeRepartitioned(table: Table<T>, partitions: Int = 12): KTable<T> {
        val repartition = Repartitioned
            .with(table.sourceTopic.keySerde, table.sourceTopic.valueSerde)
            .withNumberOfPartitions(partitions)
            .withName(table.sourceTopicName)

        val stream = consumeWithLogging(table.sourceTopic)
            .repartition(repartition)
            .addProcessor(LogProduceTableProcessor(table))
            .toTable(
                Named.`as`("${table.sourceTopicName}-to-table"),
                materialized(table.stateStoreName, table.sourceTopic)
            )

        return KTable(table, stream)
    }

    fun <T : Any> consume(
        topic: Topic<T>,
        onEach: (key: String, value: T?, metadata: ProcessorMetadata) -> Unit,
    ): ConsumedStream<T> {
        val stream = consumeWithLogging(topic)

        stream
            .addProcessor(MetadataProcessor(topic))
            .foreach { _, (kv, metadata) -> onEach(kv.key, kv.value, metadata) }

        val consumedWithoutTombstones = stream.skipTombstone(topic)
        return ConsumedStream(topic, consumedWithoutTombstones) { "from-${topic.name}" }
    }

    fun registerInternalTopology(stream: Streams) {
        stream.registerInternalTopology(builder.build())
    }

    internal fun buildInternalTopology() = builder.build()

    private fun <T : Any> consumeWithLogging(topic: Topic<T>): KStream<String, T?> =
        builder
            .stream(topic.name, topic.consumed("consume-${topic.name}"))
            .addProcessor(LogConsumeTopicProcessor(topic))
}

fun topology(init: Topology.() -> Unit): Topology = Topology().apply(init)

private fun <T : Any> materialized(
    storeName: String,
    topic: Topic<T>
): Materialized<String, T?, KeyValueStore<Bytes, ByteArray>> =
    Materialized.`as`<String, T, KeyValueStore<Bytes, ByteArray>>(storeName)
        .withKeySerde(topic.keySerde)
        .withValueSerde(topic.valueSerde)
