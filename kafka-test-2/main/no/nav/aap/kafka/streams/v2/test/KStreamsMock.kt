package no.nav.aap.kafka.streams.v2.test

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KtorKafkaMetrics
import no.nav.aap.kafka.streams.v2.*
import no.nav.aap.kafka.streams.v2.config.StreamsConfig
import no.nav.aap.kafka.streams.v2.consumer.ConsumerConfig
import no.nav.aap.kafka.streams.v2.visual.TopologyVisulizer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.streams.StreamsConfig.*
import org.apache.kafka.streams.TopologyTestDriver

class KStreamsMock : KStreams {
    private lateinit var internalStreams: TopologyTestDriver
    private lateinit var internalTopology: org.apache.kafka.streams.Topology

    override fun connect(topology: Topology, config: StreamsConfig, registry: MeterRegistry) {
        topology.registerInternalTopology(this)

        val testProperties = config.streamsProperties().apply {
            this[STATE_DIR_CONFIG] = "build/kafka-streams/state"
            this[MAX_TASK_IDLE_MS_CONFIG] = MAX_TASK_IDLE_MS_DISABLED
        }

        internalStreams = TopologyTestDriver(internalTopology, testProperties)
        KtorKafkaMetrics(registry, internalStreams::metrics)
    }

    override fun ready(): Boolean = true

    override fun live(): Boolean = true

    override fun visulize(): TopologyVisulizer = TopologyVisulizer(internalTopology)
    override fun registerInternalTopology(internalTopology: org.apache.kafka.streams.Topology) {
        this.internalTopology = internalTopology
    }

    override fun <T : Any> getStore(table: Table<T>): StateStore<T> =
        StateStore(internalStreams.getTimestampedKeyValueStore(table.stateStoreName))

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

    private val producers: MutableMap<Topic<*>, MockProducer<String, *>> = mutableMapOf()

    @Suppress("UNCHECKED_CAST")
    override fun <V : Any> createProducer(streamsConfig: StreamsConfig, topic: Topic<V>) = producers.getOrPut(topic) {
        MockProducer(true, topic.keySerde.serializer(), topic.valueSerde.serializer())
    } as MockProducer<String, V>

    @Suppress("UNCHECKED_CAST")
    fun <V : Any> getProducer(topic: Topic<V>) = producers[topic] as MockProducer<String, V>

    override fun <V : Any> createConsumer(
        streamsConfig: StreamsConfig,
        topic: Topic<V>,
        groupIdSuffix: Int,
        offsetResetPolicy: ConsumerConfig.OffsetResetPolicy
    ): Consumer<String, V> {
        val internalOffsetResetPolicy = enumValueOf<OffsetResetStrategy>(offsetResetPolicy.name.uppercase())
        return MockConsumer(internalOffsetResetPolicy)
    }

    override fun close() {
        producers.clear()
        internalStreams.close()
    }
}
