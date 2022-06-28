package no.nav.aap.kafka.streams.test

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KtorKafkaMetrics
import no.nav.aap.kafka.KafkaConfig
import no.nav.aap.kafka.plus
import no.nav.aap.kafka.streams.KStreams
import no.nav.aap.kafka.streams.Store
import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.streams.*
import java.util.*

class KafkaStreamsMock : KStreams {
    private var schemaRegistryUrl: String? = null

    lateinit var streams: TopologyTestDriver

    override fun connect(config: KafkaConfig, registry: MeterRegistry, topology: Topology) {
        val properties = config.streamsProperties() + config.sslProperties() + config.schemaProperties() + testConfig
        streams = TopologyTestDriver(topology, properties)
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

    private val testConfig = Properties().apply {
        this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state"
        this[StreamsConfig.MAX_TASK_IDLE_MS_CONFIG] = StreamsConfig.MAX_TASK_IDLE_MS_DISABLED
    }

    // Unique schema reg url for tests running in parallell without coliding on schema.drop
    private fun uniqueParallellTestSchemaReg(config: KafkaConfig) {
        config.schemaProperties()[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG]?.let {
            schemaRegistryUrl = "$it/${UUID.randomUUID()}"
        }
    }
}
