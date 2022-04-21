package no.nav.aap.kafka.streams.test

import no.nav.aap.kafka.KafkaConfig
import no.nav.aap.kafka.plus
import no.nav.aap.kafka.streams.Kafka
import no.nav.aap.kafka.streams.Store
import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.streams.*
import java.util.*

class KStreamsMock : Kafka {
    lateinit var streams: TopologyTestDriver

    override fun start(kafkaConfig: KafkaConfig, streamsBuilder: StreamsBuilder.() -> Unit) {
        val topology = StreamsBuilder().apply(streamsBuilder).build()
        streams = TopologyTestDriver(topology, kafkaConfig.consumer + kafkaConfig.producer + testConfig)
    }

    inline fun <reified V : Any> inputTopic(topic: Topic<V>): TestInputTopic<String, V> =
        streams.createInputTopic(topic.name, topic.keySerde.serializer(), topic.valueSerde.serializer())

    inline fun <reified V : Any> outputTopic(topic: Topic<V>): TestOutputTopic<String, V> =
        streams.createOutputTopic(topic.name, topic.keySerde.deserializer(), topic.valueSerde.deserializer())

    override fun isReady() = true
    override fun isLive() = true
    override fun <V> getStore(name: String): Store<V> = streams.getKeyValueStore(name)
    override fun close() = streams.close()

    private val testConfig = Properties().apply {
        this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state"
        this[StreamsConfig.MAX_TASK_IDLE_MS_CONFIG] = StreamsConfig.MAX_TASK_IDLE_MS_DISABLED
    }
}
