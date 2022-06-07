package no.nav.aap.kafka.streams

import no.nav.aap.kafka.serde.json.JsonSerde
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TopologyDescription
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class KafkaStreamsTest {

    @Test
    fun `consume() and produce() a Topic`() {
        val sourceTopic = Topic("source", JsonSerde.jackson<String>())
        val sinkTopic = Topic("destination", JsonSerde.jackson<String>())

        val topology = StreamsBuilder().apply {
            consume(sourceTopic)
                .mapValues { _, value -> "$value processed" }
                .produce(sinkTopic, "produced")
        }.build()

        val testDriver = TopologyTestDriver(topology)

        val inputTopic = testDriver.createInputTopic(
            sourceTopic.name,
            sourceTopic.keySerde.serializer(),
            sourceTopic.valueSerde.serializer()
        )

        val outputTopic = testDriver.createOutputTopic(
            sinkTopic.name,
            sinkTopic.keySerde.deserializer(),
            sinkTopic.valueSerde.deserializer()
        )

        inputTopic.pipeInput("hello")
        assertEquals("hello processed", outputTopic.readValue())

        val nodes = topology.describe().subtopologies().flatMap { sub -> sub.nodes() }
        val sourceName = nodes.filterIsInstance<TopologyDescription.Source>().map { it.name() }.single()
        val sinkName = nodes.filterIsInstance<TopologyDescription.Sink>().map { it.name() }.single()

        assertEquals("consume-source", sourceName)
        assertEquals("produced", sinkName)
    }
}
