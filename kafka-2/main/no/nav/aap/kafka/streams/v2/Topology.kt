package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.v2.extension.skipTombstone
import no.nav.aap.kafka.streams.v2.processor.LogConsumeTopicProcessor
import no.nav.aap.kafka.streams.v2.processor.Processor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.ProcessorMetadata
import no.nav.aap.kafka.streams.v2.stream.ConsumedKStream
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream

class Topology internal constructor() {
    private val builder = StreamsBuilder()

    fun <T : Any> consume(topic: Topic<T>, logValue: Boolean = false): ConsumedKStream<T> {
        val consumed = consumeAll(topic, logValue).skipTombstone(topic)
        return ConsumedKStream(topic, consumed) { "from-${topic.name}" }
    }

    fun <T : Any> consume(
        topic: Topic<T>,
        logValue: Boolean = false,
        onEach: (key: String, value: T?, metadata: ProcessorMetadata) -> Unit,
    ): ConsumedKStream<T> {
        val consumedWithTombstones = consumeAll(topic, logValue)

        consumedWithTombstones
            .addProcessor(MetadataProcessor(topic))
            .foreach { _, (kv, metadata) -> onEach(kv.key, kv.value, metadata) }

        val consumedWithoutTombstones = consumedWithTombstones.skipTombstone(topic)
        return ConsumedKStream(topic, consumedWithoutTombstones) { "from-${topic.name}" }
    }

    private fun <T : Any> consumeAll(topic: Topic<T>, logValue: Boolean): KStream<String, T?> =
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
