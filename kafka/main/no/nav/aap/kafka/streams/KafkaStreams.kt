package no.nav.aap.kafka.streams

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import no.nav.aap.kafka.KFactory
import no.nav.aap.kafka.KafkaConfig
import no.nav.aap.kafka.plus
import no.nav.aap.kafka.streams.transformer.TraceLogTransformer
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams.State.*
import org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.KafkaStreams as ApacheKafkaStreams

typealias Store<V> = ReadOnlyKeyValueStore<String, V>

interface KStreams : KFactory, AutoCloseable {
    fun connect(config: KafkaConfig, registry: MeterRegistry, topology: Topology)
    fun isReady(): Boolean
    fun isLive(): Boolean
    fun <V> getStore(name: String): Store<V>
}

object KafkaStreams : KStreams {
    private lateinit var streams: ApacheKafkaStreams
    private var isInitiallyStarted: Boolean = false

    override fun connect(config: KafkaConfig, registry: MeterRegistry, topology: Topology) {
        val properties = config.streamsProperties() + config.sslProperties() + config.schemaProperties()
        streams = ApacheKafkaStreams(topology, properties).apply {
            setUncaughtExceptionHandler(ProcessingExceptionHandler())
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

fun <V> StreamsBuilder.consume(topic: Topic<V>, logValue: Boolean = false): KStream<String, V?> =
    stream(topic.name, topic.consumed("consume-${topic.name}"))
        .transformValues(
            ValueTransformerWithKeySupplier {
                TraceLogTransformer<String, V>(
                    message = "Konsumerer Topic",
                    logValue = logValue,
                )
            }, named("log-consume-${topic.name}")
        )

fun <V> StreamsBuilder.globalTable(table: Table<V>): GlobalKTable<String, V> =
    globalTable(table.source.name, table.source.consumed("${table.name}-as-globaltable"))

