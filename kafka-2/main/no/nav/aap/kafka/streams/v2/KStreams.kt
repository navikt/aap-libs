package no.nav.aap.kafka.streams.v2

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import no.nav.aap.kafka.streams.v2.config.KStreamsConfig
import no.nav.aap.kafka.streams.v2.exception.ProcessingExceptionHandler
import no.nav.aap.kafka.streams.v2.listener.RestoreListener
import no.nav.aap.kafka.streams.v2.visual.Mermaid
import no.nav.aap.kafka.streams.v2.visual.PlantUML
import org.apache.kafka.streams.KafkaStreams.State.*

interface KStreams {
    fun connect(
        topology: Topology,
        config: KStreamsConfig,
        registry: MeterRegistry,
    )

    fun ready(): Boolean
    fun live(): Boolean
    fun toUML(): String
    fun toMermaid(): String
    fun registerInternalTopology(internalTopology: org.apache.kafka.streams.Topology)
}

class KafkaStreams : KStreams {
    private var initiallyStarted: Boolean = false

    private lateinit var internalStreams: org.apache.kafka.streams.KafkaStreams
    private lateinit var internalTopology: org.apache.kafka.streams.Topology

    override fun connect(
        topology: Topology,
        config: KStreamsConfig,
        registry: MeterRegistry,
    ) {
        topology.registerInternalTopology(this)

        internalStreams = org.apache.kafka.streams.KafkaStreams(internalTopology, config.streamsProperties())
        KafkaStreamsMetrics(internalStreams).bindTo(registry)
        internalStreams.setUncaughtExceptionHandler(ProcessingExceptionHandler())
        internalStreams.setStateListener { state, _ -> if (state == RUNNING) initiallyStarted = true }
        internalStreams.setGlobalStateRestoreListener(RestoreListener())
        internalStreams.start()
    }

    override fun ready(): Boolean = initiallyStarted && internalStreams.state() in listOf(CREATED, REBALANCING, RUNNING)
    override fun live(): Boolean = initiallyStarted && internalStreams.state() != ERROR
    override fun toUML(): String = PlantUML.generate(internalTopology)
    override fun toMermaid(): String = Mermaid.generate("", internalTopology)
    override fun registerInternalTopology(internalTopology: org.apache.kafka.streams.Topology) {
        this.internalTopology = internalTopology
    }
}
