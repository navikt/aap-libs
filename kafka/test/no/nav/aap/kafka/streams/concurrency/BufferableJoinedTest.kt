package no.nav.aap.kafka.streams.concurrency

import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.BufferableTopic
import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.Topic
import no.nav.aap.kafka.streams.extension.consume
import no.nav.aap.kafka.streams.extension.filterNotNull
import no.nav.aap.kafka.streams.extension.join
import no.nav.aap.kafka.streams.extension.produce
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BufferableJoinedTest {
    @Test
    fun `Buffrer og henter ut buffret versjon da den er nyest`() {
        val buffer = RaceConditionBuffer<String, TestBufferable>()
        val right = BufferableTopic("right", JsonSerde.jackson(), buffer)
        val table = Table("right", right)
        val left = Topic<String>("left", StringSerde())

        val topology = StreamsBuilder().apply {
            val ktable = consume(right).produce(table)
            consume(left).filterNotNull("tombstone")
                .join(left with right, ktable) { l: String, r: TestBufferable -> r.copy(value = l) }
                .produce(right, "sink")
        }.build()

        val kafka = TopologyTestDriver(topology)
        val rightInputTopic = kafka.inputTopic(right)
        val leftInputTopic = kafka.inputTopic(left)
        val sinkOutputTopic = kafka.outputTopic(right)
        rightInputTopic.pipeInput("123", TestBufferable(1, ""))
        leftInputTopic.pipeInput("123", "A")
        assertEquals("1:A", sinkOutputTopic.readValue().toString())
    }

    data class TestBufferable(val sekvensnummer: Int, val value: String) : Bufferable<TestBufferable> {
        override fun erNyere(other: TestBufferable): Boolean = this.sekvensnummer > other.sekvensnummer
        override fun toString(): String = "$sekvensnummer:$value"
    }
}

private fun <V> TopologyTestDriver.inputTopic(topic: Topic<V>) =
    createInputTopic(topic.name, topic.keySerde.serializer(), topic.valueSerde.serializer())

private fun <V> TopologyTestDriver.outputTopic(topic: Topic<V>) =
    createOutputTopic(topic.name, topic.keySerde.deserializer(), topic.valueSerde.deserializer())