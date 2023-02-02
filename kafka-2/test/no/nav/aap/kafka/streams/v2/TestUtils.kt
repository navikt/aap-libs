package no.nav.aap.kafka.streams.v2

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver

internal object Topics {
    val A = Topic("A", StringSerde())
    val B = Topic("B", StringSerde())
    val C = Topic("C", StringSerde())
    val D = Topic("D", StringSerde())
}

internal object Tables {
    val B = Table(Topics.B)
}

internal fun <V> TopologyTestDriver.inputTopic(topic: Topic<V>): TestInputTopic<String, V> =
    createInputTopic(topic.name, topic.keySerde.serializer(), topic.valueSerde.serializer())

internal fun <V> TopologyTestDriver.outputTopic(topic: Topic<V>): TestOutputTopic<String, V> =
    createOutputTopic(topic.name, topic.keySerde.deserializer(), topic.valueSerde.deserializer())

internal fun <V> TestInputTopic<String, V>.produce(key: String, value: V): TestInputTopic<String, V> =
    pipeInput(key, value).let { this }

internal fun kafka(topology: Topology) = TopologyTestDriver(topology.build())
