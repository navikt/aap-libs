package no.nav.aap.kafka.streams.v2

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import no.nav.aap.kafka.streams.v2.config.KStreamsConfig
import no.nav.aap.kafka.streams.v2.exception.ProcessingExceptionHandler
import no.nav.aap.kafka.streams.v2.listener.RestoreListener
import org.apache.kafka.streams.KafkaStreams.State.*

interface KStreams {
    fun connect()
    fun ready(): Boolean
    fun live(): Boolean
}

class KafkaStreams(
    private val topology: Topology,
    private val config: KStreamsConfig,
    private val registry: MeterRegistry,
) : KStreams {
    private var initiallyStarted: Boolean = false

    private lateinit var internalStreams: org.apache.kafka.streams.KafkaStreams
    private lateinit var internalTopology: org.apache.kafka.streams.Topology

    override fun connect() {
        internalTopology = topology.build()
        internalStreams = org.apache.kafka.streams.KafkaStreams(
            internalTopology,
            config.streamsProperties()
        )
        KafkaStreamsMetrics(internalStreams).bindTo(registry)
        internalStreams.setUncaughtExceptionHandler(ProcessingExceptionHandler())
        internalStreams.setStateListener { state, _ -> if (state == RUNNING) initiallyStarted = true }
        internalStreams.setGlobalStateRestoreListener(RestoreListener())
        internalStreams.start()
    }

    override fun ready(): Boolean = initiallyStarted && internalStreams.state() in listOf(CREATED, REBALANCING, RUNNING)
    override fun live(): Boolean = initiallyStarted && internalStreams.state() != ERROR
}
