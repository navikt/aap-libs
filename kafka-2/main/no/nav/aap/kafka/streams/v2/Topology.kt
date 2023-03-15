package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.concurrency.Bufferable
import no.nav.aap.kafka.streams.v2.concurrency.RaceConditionBuffer
import no.nav.aap.kafka.streams.v2.extension.skipTombstone
import no.nav.aap.kafka.streams.v2.processor.LogConsumeTopicProcessor
import no.nav.aap.kafka.streams.v2.processor.LogProduceTableProcessor
import no.nav.aap.kafka.streams.v2.processor.Processor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.ProcessorMetadata
import no.nav.aap.kafka.streams.v2.stream.ConsumedKStream
import no.nav.aap.kafka.streams.v2.stream.materialized
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import java.time.Duration

class Topology internal constructor() {
    private val builder = StreamsBuilder()

    fun test() {
        val søkere = builder.stream<String?, String?>("søkere")
        val table = søkere.toTable(Named.`as`("lol"))

        val b: KStream<String, String> = builder.stream("b")
        val c: KStream<String, String> = builder.stream("c")

        b.mapValues { _, value -> listOf(value.toInt()) }.to("hendelser")
        c.mapValues { _, value -> listOf(value.toInt()) }.to("hendelser")

        builder.stream<String, List<Int>>("hendelser")
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(500)))

            .reduce { value1, value2 -> value1 + value2 }
            .toStream()
            .selectKey { key, _ -> key.key() }
            .join(table) { hendelser, søker -> søker.plus(hendelser.joinToString()) }
            .to("søkere")
    }

    fun <T : Any> consume(topic: Topic<T>, logValue: Boolean = false): ConsumedKStream<T> {
        val consumed = consumeWithTombstone(topic, logValue).skipTombstone(topic)
        return ConsumedKStream(topic, consumed) { "from-${topic.name}" }
    }

    fun <T : Any> consume(table: Table<T>, logValues: Boolean = false): KTable<T> {
        val consumed = consumeWithTombstone(table.sourceTopic, logValues)
            .addProcessor(LogProduceTableProcessor("log-produced-${table.sourceTopicName}", table, logValues))
            .toTable(
                Named.`as`("${table.sourceTopicName}-to-table"),
                materialized(table.stateStoreName, table.sourceTopic)
            )
        return KTable(table, consumed)
    }

    fun <T : Bufferable<T>> consume(
        table: Table<T>,
        buffer: RaceConditionBuffer<T>,
        logValues: Boolean = false
    ): KTable<T> {
        val consumed = consumeWithTombstone(table.sourceTopic, logValues)

        consumed.filter { _, value -> value == null }.foreach { key, _ -> buffer.slett(key) }

        val internalKtable = consumed
            .addProcessor(LogProduceTableProcessor("log-produced-${table.sourceTopicName}", table, logValues))
            .toTable(
                Named.`as`("${table.sourceTopicName}-to-table"),
                materialized(table.stateStoreName, table.sourceTopic)
            )
        return KTable(table, internalKtable)
    }

    fun <T : Any> consumeRepartitioned(table: Table<T>, partitions: Int = 12, logValues: Boolean = false): KTable<T> {
        val repartition = Repartitioned
            .with(table.sourceTopic.keySerde, table.sourceTopic.valueSerde)
            .withNumberOfPartitions(partitions)
            .withName(table.sourceTopicName)

        val consumed = consumeWithTombstone(table.sourceTopic, logValues)
            .addProcessor(LogProduceTableProcessor("log-produced-${table.sourceTopicName}", table, logValues))
            .repartition(repartition)
            .toTable(
                Named.`as`("${table.sourceTopicName}-to-table"),
                materialized(table.stateStoreName, table.sourceTopic)
            )

        return KTable(table, consumed)
    }

    fun <T : Any> consume(
        topic: Topic<T>,
        logValue: Boolean = false,
        onEach: (key: String, value: T?, metadata: ProcessorMetadata) -> Unit,
    ): ConsumedKStream<T> {
        val consumedWithTombstones = consumeWithTombstone(topic, logValue)

        consumedWithTombstones
            .addProcessor(MetadataProcessor(topic))
            .foreach { _, (kv, metadata) -> onEach(kv.key, kv.value, metadata) }

        val consumedWithoutTombstones = consumedWithTombstones.skipTombstone(topic)
        return ConsumedKStream(topic, consumedWithoutTombstones) { "from-${topic.name}" }
    }

    private fun <T : Any> consumeWithTombstone(topic: Topic<T>, logValue: Boolean): KStream<String, T?> =
        builder
            .stream(topic.name, topic.consumed("consume-${topic.name}"))
            .addProcessor(
                LogConsumeTopicProcessor(
                    named = "log-consume-${topic.name}",
                    logValue = logValue
                )
            )

    fun registerInternalTopology(stream: KStreams) {
        stream.registerInternalTopology(builder.build())
    }

    internal fun buildInternalTopology() = builder.build()
}

fun topology(init: Topology.() -> Unit): Topology = Topology().apply(init)

private class MetadataProcessor<T : Any>(
    topic: Topic<T>,
) : Processor<T?, Pair<KeyValue<String, T?>, ProcessorMetadata>>(
    "from-${topic.name}-enrich-metadata",
) {
    override fun process(
        metadata: ProcessorMetadata,
        keyValue: KeyValue<String, T?>,
    ): Pair<KeyValue<String, T?>, ProcessorMetadata> =
        keyValue to metadata
}
