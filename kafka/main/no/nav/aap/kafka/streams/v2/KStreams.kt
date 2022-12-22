package no.nav.aap.kafka.streams.v2

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import no.nav.aap.kafka.streams.KStreamsConfig
import no.nav.aap.kafka.streams.handler.ProcessingExceptionHandler
import no.nav.aap.kafka.streams.store.RestoreListener
import org.apache.kafka.streams.KafkaStreams.State.CREATED
import org.apache.kafka.streams.KafkaStreams.State.ERROR
import org.apache.kafka.streams.KafkaStreams.State.REBALANCING
import org.apache.kafka.streams.KafkaStreams.State.RUNNING

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
    private var isInitiallyStarted: Boolean = false

    private lateinit var streams: org.apache.kafka.streams.KafkaStreams

    override fun connect() {
        streams = org.apache.kafka.streams.KafkaStreams(
            topology.build(),
            config.streamsProperties()
        )
        KafkaStreamsMetrics(streams).bindTo(registry)
        streams.setUncaughtExceptionHandler(ProcessingExceptionHandler())
        streams.setStateListener { state, _ -> if (state == RUNNING) isInitiallyStarted = true }
        streams.setGlobalStateRestoreListener(RestoreListener())
        streams.start()
    }

    override fun ready(): Boolean = isInitiallyStarted && streams.state() in listOf(CREATED, REBALANCING, RUNNING)
    override fun live(): Boolean = isInitiallyStarted && streams.state() != ERROR
}
