package no.nav.aap.kafka.streams.v2.test

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KtorKafkaMetrics
import no.nav.aap.kafka.streams.v2.KStreams
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.Topology
import no.nav.aap.kafka.streams.v2.config.KStreamsConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver

class KStreamsMock : KStreams, AutoCloseable {
    private lateinit var internalStreams: TopologyTestDriver
    private lateinit var internalTopology: org.apache.kafka.streams.Topology

    override fun connect(topology: Topology, config: KStreamsConfig, registry: MeterRegistry) {
        topology.registerInternalTopology(this)

        val testProperties = config.streamsProperties().apply {
            this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state"
            this[StreamsConfig.MAX_TASK_IDLE_MS_CONFIG] = StreamsConfig.MAX_TASK_IDLE_MS_DISABLED
        }

        internalStreams = TopologyTestDriver(internalTopology, testProperties)
        KtorKafkaMetrics(registry, internalStreams::metrics)
    }

    override fun ready(): Boolean = true

    override fun live(): Boolean = true

    override fun toUML(): String = no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(internalTopology)
    override fun registerInternalTopology(internalTopology: org.apache.kafka.streams.Topology) {
        this.internalTopology = internalTopology
    }

    fun <V : Any> testTopic(topic: Topic<V>): TestTopic<V> =
        TestTopic(
            input = internalStreams.createInputTopic(
                topic.name,
                topic.keySerde.serializer(),
                topic.valueSerde.serializer()
            ),
            output = internalStreams.createOutputTopic(
                topic.name,
                topic.keySerde.deserializer(),
                topic.valueSerde.deserializer()
            )
        )

    override fun close() {
        internalStreams.close()
    }
}
