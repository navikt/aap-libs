package no.nav.aap.kafka.streams.test

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
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
    lateinit var streams: TopologyTestDriver
    private var schemaRegistryUrl: String? = null

    override fun start(config: KafkaConfig, registry: MeterRegistry, builder: StreamsBuilder.() -> Unit) {
        val topology = StreamsBuilder().apply(builder).build()
        streams = TopologyTestDriver(topology, config.consumer + config.producer + testConfig)
        config.schemaRegistryUrl?.let { schemaRegistryUrl = "$it/${UUID.randomUUID()}" }
        KtorKafkaMetrics(registry, streams::metrics)
    }

    inline fun <reified V : Any> inputTopic(topic: Topic<V>): TestInputTopic<String, V> =
        streams.createInputTopic(topic.name, topic.keySerde.serializer(), topic.valueSerde.serializer())

    inline fun <reified V : Any> outputTopic(topic: Topic<V>): TestOutputTopic<String, V> =
        streams.createOutputTopic(topic.name, topic.keySerde.deserializer(), topic.valueSerde.deserializer())

    override fun isReady() = true
    override fun isLive() = true
    override fun <V> getStore(name: String): Store<V> = streams.getKeyValueStore(name)

    override fun <V : Any> createConsumer(config: KafkaConfig, topic: Topic<V>) = MockConsumer<String, V>(EARLIEST)
    override fun <V : Any> createProducer(config: KafkaConfig, topic: Topic<V>) =
        MockProducer(true, topic.keySerde.serializer(), topic.valueSerde.serializer())

    override fun close() {
        streams.close()
        schemaRegistryUrl?.let { MockSchemaRegistry.dropScope(it) }
    }

    private val testConfig = Properties().apply {
        this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state"
        this[StreamsConfig.MAX_TASK_IDLE_MS_CONFIG] = StreamsConfig.MAX_TASK_IDLE_MS_DISABLED
    }
}