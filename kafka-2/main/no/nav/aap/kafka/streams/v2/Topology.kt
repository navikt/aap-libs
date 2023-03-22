package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.concurrency.Bufferable
import no.nav.aap.kafka.streams.v2.concurrency.RaceConditionBuffer
import no.nav.aap.kafka.streams.v2.extension.materialized
import no.nav.aap.kafka.streams.v2.extension.repartitioned
import no.nav.aap.kafka.streams.v2.extension.skipTombstone
import no.nav.aap.kafka.streams.v2.extension.toKtable
import no.nav.aap.kafka.streams.v2.processor.LogConsumeTopicProcessor
import no.nav.aap.kafka.streams.v2.processor.LogProduceTableProcessor
import no.nav.aap.kafka.streams.v2.processor.MetadataProcessor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.ProcessorMetadata
import no.nav.aap.kafka.streams.v2.stream.ConsumedStream
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named

private fun <T : Any> namedFrom(topic: Topic<T>): () -> String = { "from-${topic.name}" }

class Topology internal constructor() {
    private val builder = StreamsBuilder()

    fun <T : Any> consume(topic: Topic<T>): ConsumedStream<T> {
        val consumed = consumeWithLogging<T?>(topic).skipTombstone(topic)
        return ConsumedStream(topic, consumed, namedFrom(topic))
    }

    fun <T : Any> consume(table: Table<T>): KTable<T> {
        val stream = consumeWithLogging<T?>(table.sourceTopic)
        return stream.toKtable(table)
    }

    fun <T : Bufferable<T>> consume(table: Table<T>, buffer: RaceConditionBuffer<T>): KTable<T> {
        val stream = consumeWithLogging<T?>(table.sourceTopic)
        stream.filter { _, value -> value == null }.foreach { key, _ -> buffer.slett(key) }
        return stream.toKtable(table)
    }

    fun <T : Any> consumeRepartitioned(table: Table<T>, partitions: Int = 12): KTable<T> {
        val internalKTable = consumeWithLogging<T?>(table.sourceTopic)
            .repartition(repartitioned(table, partitions))
            .addProcessor(LogProduceTableProcessor(table))
            .toTable(
                Named.`as`("${table.sourceTopicName}-to-table"),
                materialized(table)
            )

        return KTable(table = table, internalKTable = internalKTable)
    }

    /** Sometimes it is necessary to consume the same topic twice, e.g. mocking a response */
    fun <T : Any> consumeAgain(topic: Topic<T>, namedPrefix: String = "mock"): ConsumedStream<T> {
        val consumed = consumeWithLogging(topic, namedPrefix).skipTombstone(topic, namedPrefix)
        val prefixedNamedSupplier = { "$namedPrefix-${namedFrom(topic)}" }
        return ConsumedStream(topic, consumed, prefixedNamedSupplier)
    }

    fun <T : Any> consume(
        topic: Topic<T>,
        onEach: (key: String, value: T?, metadata: ProcessorMetadata) -> Unit,
    ): ConsumedStream<T> {
        val stream = consumeWithLogging<T?>(topic)
        stream.addProcessor(MetadataProcessor(topic)).foreach { _, (kv, metadata) ->
            onEach(kv.key, kv.value, metadata)
        }
        return ConsumedStream(topic, stream.skipTombstone(topic), namedFrom(topic))
    }

    fun registerInternalTopology(stream: Streams) {
        stream.registerInternalTopology(builder.build())
    }

    internal fun buildInternalTopology() = builder.build()

    private fun <T> consumeWithLogging(topic: Topic<T & Any>): KStream<String, T> = consumeWithLogging(topic, "")

    private fun <T> consumeWithLogging(topic: Topic<T & Any>, namedSuffix: String): KStream<String, T> {
        val consumeInternal = topic.consumed("consume-${topic.name}$namedSuffix")
        val consumeLogger = LogConsumeTopicProcessor<T>(topic, namedSuffix)
        return builder.stream(topic.name, consumeInternal).addProcessor(consumeLogger)
    }
}

fun topology(init: Topology.() -> Unit): Topology = Topology().apply(init)


