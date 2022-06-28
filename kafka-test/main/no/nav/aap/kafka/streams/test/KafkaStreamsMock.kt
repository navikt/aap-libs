package no.nav.aap.kafka.streams.test

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KtorKafkaMetrics
import no.nav.aap.kafka.streams.KStreams
import no.nav.aap.kafka.streams.KStreamsConfig
import no.nav.aap.kafka.streams.Store
import no.nav.aap.kafka.streams.Topic
import no.nav.aap.kafka.vanilla.KafkaConfig
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.streams.*
import java.util.*

class KafkaStreamsMock : KStreams {
    var schemaRegistryUrl: String? = null
        private set

    lateinit var streams: TopologyTestDriver

    override fun connect(config: KStreamsConfig, registry: MeterRegistry, topology: Topology) {
        val testProperties = config.streamsProperties().apply {
            this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state"
            this[StreamsConfig.MAX_TASK_IDLE_MS_CONFIG] = StreamsConfig.MAX_TASK_IDLE_MS_DISABLED
        }

        streams = TopologyTestDriver(topology, testProperties)
        uniqueParallellTestSchemaReg(config)
        KtorKafkaMetrics(registry, streams::metrics)
    }

    inline fun <reified V : Any> inputTopic(topic: Topic<V>): TestInputTopic<String, V> =
        streams.createInputTopic(topic.name, topic.keySerde.serializer(), topic.valueSerde.serializer())

    inline fun <reified V : Any> outputTopic(topic: Topic<V>): TestOutputTopic<String, V> =
        streams.createOutputTopic(topic.name, topic.keySerde.deserializer(), topic.valueSerde.deserializer())

    override fun isReady() = true

    override fun isLive() = true
    override fun <V> getStore(name: String): Store<V> = streams.getKeyValueStore(name)
    private val producers: MutableMap<Topic<*>, MockProducer<String, *>> = mutableMapOf()

    override fun <V : Any> createConsumer(config: KafkaConfig, topic: Topic<V>) = MockConsumer<String, V>(EARLIEST)

    @Suppress("UNCHECKED_CAST")
    override fun <V : Any> createProducer(config: KafkaConfig, topic: Topic<V>) = producers.getOrPut(topic) {
        MockProducer(true, topic.keySerde.serializer(), topic.valueSerde.serializer())
    } as MockProducer<String, V>

    @Suppress("UNCHECKED_CAST")
    fun <V : Any> getProducer(topic: Topic<V>) = producers[topic] as MockProducer<String, V>

    override fun close() {
        producers.clear()
        streams.close()
        schemaRegistryUrl?.let { MockSchemaRegistry.dropScope(it) }
    }

    // Unique schema reg url for tests running in parallell without coliding on schema.drop
    private fun uniqueParallellTestSchemaReg(config: KStreamsConfig) {
        config.streamsProperties()["schema.registry.url"]?.let {
            schemaRegistryUrl = "$it/${UUID.randomUUID()}"
        }
    }
}
