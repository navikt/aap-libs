package libs.kafka

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import libs.kafka.client.ConsumerFactory
import libs.kafka.client.ProducerFactory
import libs.kafka.visual.TopologyVisulizer
import org.apache.kafka.streams.KafkaStreams.State.*
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp

interface Streams : ProducerFactory, ConsumerFactory, AutoCloseable {
    fun connect(topology: Topology, config: StreamsConfig, registry: MeterRegistry)
    fun ready(): Boolean
    fun live(): Boolean
    fun visulize(): TopologyVisulizer
    fun registerInternalTopology(internalTopology: org.apache.kafka.streams.Topology)
    fun <T : Any> getStore(table: Table<T>): StateStore<T>
}

class KafkaStreams : Streams {
    private var initiallyStarted: Boolean = false

    private lateinit var internalStreams: org.apache.kafka.streams.KafkaStreams
    private lateinit var internalTopology: org.apache.kafka.streams.Topology

    override fun connect(
        topology: Topology,
        config: StreamsConfig,
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
    override fun visulize(): TopologyVisulizer = TopologyVisulizer(internalTopology)
    override fun close() = internalStreams.close()

    override fun registerInternalTopology(internalTopology: org.apache.kafka.streams.Topology) {
        this.internalTopology = internalTopology
    }

    override fun <T : Any> getStore(table: Table<T>): StateStore<T> = StateStore(
        internalStreams.store(
            StoreQueryParameters.fromNameAndType<ReadOnlyKeyValueStore<String, ValueAndTimestamp<T>>>(
                table.stateStoreName,
                QueryableStoreTypes.keyValueStore()
            )
        )
    )
}
