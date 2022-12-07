package no.nav.aap.kafka.streams

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import no.nav.aap.kafka.streams.handler.ProcessingExceptionHandler
import no.nav.aap.kafka.streams.store.RestoreListener
import no.nav.aap.kafka.vanilla.KafkaFactory
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams.State.CREATED
import org.apache.kafka.streams.KafkaStreams.State.ERROR
import org.apache.kafka.streams.KafkaStreams.State.REBALANCING
import org.apache.kafka.streams.KafkaStreams.State.RUNNING
import org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.KafkaStreams as ApacheKafkaStreams

typealias Store<V> = ReadOnlyKeyValueStore<String, V>

interface KStreams : KafkaFactory, AutoCloseable {
    fun connect(config: KStreamsConfig, registry: MeterRegistry, topology: Topology)

    fun connect(
        config: KStreamsConfig,
        registry: MeterRegistry,
        errorHandler: StreamsUncaughtExceptionHandler,
        topology: Topology
    )

    fun isReady(): Boolean
    fun isLive(): Boolean
    fun <V> getStore(name: String): Store<V>
}

object KafkaStreams : KStreams {
    private lateinit var streams: ApacheKafkaStreams
    private var isInitiallyStarted: Boolean = false

    override fun connect(config: KStreamsConfig, registry: MeterRegistry, topology: Topology) {
        connect(
            config = config,
            registry = registry,
            errorHandler = ProcessingExceptionHandler(),
            topology = topology,
        )
    }

    override fun connect(
        config: KStreamsConfig,
        registry: MeterRegistry,
        errorHandler: StreamsUncaughtExceptionHandler,
        topology: Topology,
    ) {
        streams = ApacheKafkaStreams(topology, config.streamsProperties()).apply {
            setUncaughtExceptionHandler(errorHandler)
            setStateListener { state, _ -> if (state == RUNNING) isInitiallyStarted = true }
            setGlobalStateRestoreListener(RestoreListener())
            KafkaStreamsMetrics(this).bindTo(registry)
            start()
        }
    }

    override fun <V> getStore(name: String): Store<V> = streams.store(fromNameAndType<Store<V>>(name, keyValueStore()))
    override fun isReady() = isInitiallyStarted && streams.state() in listOf(CREATED, RUNNING, REBALANCING)
    override fun isLive() = isInitiallyStarted && streams.state() != ERROR
    override fun close() = streams.close()
}

fun named(named: String): Named = Named.`as`(named)

fun <V> materialized(storeName: String, topic: Topic<V>): Materialized<String, V?, KeyValueStore<Bytes, ByteArray>> =
    Materialized.`as`<String, V, KeyValueStore<Bytes, ByteArray>>(storeName)
        .withKeySerde(topic.keySerde)
        .withValueSerde(topic.valueSerde)
