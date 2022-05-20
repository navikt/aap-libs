package no.nav.aap.kafka.streams

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import no.nav.aap.kafka.KFactory
import no.nav.aap.kafka.KafkaConfig
import no.nav.aap.kafka.ProcessingExceptionHandler
import no.nav.aap.kafka.plus
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams.State.*
import org.apache.kafka.streams.StoreQueryParameters.fromNameAndType
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory
import org.apache.kafka.streams.KafkaStreams as ApacheKafkaStreams

typealias Store<V> = ReadOnlyKeyValueStore<String, V>

private val secureLog = LoggerFactory.getLogger("secureLog")

interface KStreams : KFactory, AutoCloseable {
    fun start(config: KafkaConfig, registry: MeterRegistry, builder: StreamsBuilder.() -> Unit)
    fun isReady(): Boolean
    fun isLive(): Boolean
    fun <V> getStore(name: String): Store<V>
}

object KafkaStreams : KStreams {
    private lateinit var streams: ApacheKafkaStreams
    private var isInitiallyStarted: Boolean = false

    override fun start(config: KafkaConfig, registry: MeterRegistry, builder: StreamsBuilder.() -> Unit) {
        val topology = StreamsBuilder().apply(builder).build()
        streams = ApacheKafkaStreams(topology, config.consumer + config.producer).apply {
            setUncaughtExceptionHandler(ProcessingExceptionHandler())
            setStateListener { state, _ -> if (state == RUNNING) isInitiallyStarted = true }
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

fun <V> materialized(storeName: String, topic: Topic<V>): Materialized<String, V, KeyValueStore<Bytes, ByteArray>> =
    Materialized.`as`<String, V, KeyValueStore<Bytes, ByteArray>>(storeName)
        .withKeySerde(topic.keySerde)
        .withValueSerde(topic.valueSerde)

fun <V> StreamsBuilder.consume(topic: Topic<V>): KStream<String, V?> =
    stream(topic.name, topic.consumed("consume-${topic.name}"))
        .peek { key, value -> secureLog.info("consumed [${topic.name}] K:$key V:$value") }

fun <V> StreamsBuilder.globalTable(table: Table<V>): GlobalKTable<String, V> =
    globalTable(table.source.name, table.source.consumed("${table.name}-as-globaltable"))
