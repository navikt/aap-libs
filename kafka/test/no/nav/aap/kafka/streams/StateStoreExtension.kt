package no.nav.aap.kafka.streams

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.extension.consume
import no.nav.aap.kafka.streams.extension.produce
import no.nav.aap.kafka.streams.store.allValues
import no.nav.aap.kafka.streams.store.scheduleMetrics
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyDescription
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

internal class StateStoreExtension {

    @Test
    fun `read all values from read only state store`() {
        val topic = Topic("source", JsonSerde.jackson<String>())
        val table = Table("table", topic)

        val topology = StreamsBuilder().apply {
            consume(topic).produce(table)
        }.build()

        val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
        val kafka = TopologyTestDriver(topology, config)
        inputTopic(kafka, topic).pipeInput("123", "hello")
        inputTopic(kafka, topic).pipeInput("456", "hello")

        val stateStore = kafka.getKeyValueStore<String, String>("${table.name}-state-store")
        val allValues = stateStore.allValues()
        assertEquals(2, allValues.size)
        assertEquals("hello", allValues.distinctBy { it }.firstOrNull())
    }

    @Test
    fun `tombstone also removes record in state store`() {
        val topic = Topic("source", JsonSerde.jackson<String>())
        val table = Table("table", topic)

        val stream = StreamsBuilder()
        stream
            .consume(topic)
            .produce(table)

        val topology = stream.build()

        val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
        val kafka = TopologyTestDriver(topology, config)
        val inputTopic = inputTopic(kafka, topic)
        inputTopic.pipeInput("123", "hello")
        inputTopic.pipeInput("456", "hello")

        val stateStore = kafka.getKeyValueStore<String, String>("${table.name}-state-store")

        assertEquals(2, stateStore.allValues().size)
        inputTopic.pipeInput(tombstone("123"))

        assertEquals(1, stateStore.allValues().size)
        assertEquals("hello", stateStore["456"])

        inputTopic.pipeInput(tombstone("456"))
        assertTrue(stateStore.allValues().isEmpty())
    }

    private fun <K, V> tombstone(key: K): TestRecord<K, V> = TestRecord(key, null)

    @Test
    fun `state store metrics generates metrics`() {
        val topic = Topic("source", JsonSerde.jackson<String>())
        val table = Table("table", topic)

        val registry = SimpleMeterRegistry()

        val stream = StreamsBuilder()
        val ktable = stream.consume(topic).produce(table)
        ktable.scheduleMetrics(table, 1.seconds, registry)
        val topology = stream.build()
        val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
        val kafka = TopologyTestDriver(topology, config)
        val inputTopic = inputTopic(kafka, topic)

        kafka.advanceWallClockTime(1.seconds.toJavaDuration())
        assertEquals(0.0, registry.get("kafka_stream_state_store_entries").gauge().value())

        inputTopic.pipeInput("123", "some message")
        kafka.advanceWallClockTime(1.seconds.toJavaDuration())
        assertEquals(1.0, registry.get("kafka_stream_state_store_entries").gauge().value())
    }

    @Test
    fun `state store metrics is named`() {
        val topic = Topic("source", JsonSerde.jackson<String>())
        val table = Table("table", topic)

        val registry = SimpleMeterRegistry()

        val stream = StreamsBuilder()
        val ktable = stream.consume(topic).produce(table)
        ktable.scheduleMetrics(table, 1.seconds, registry)
        val topology = stream.build()
        val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
        val kafka = TopologyTestDriver(topology, config)
        inputTopic(kafka, topic)

        val lastProcessorNode = topology
            .describe()
            .subtopologies()
            .flatMap(TopologyDescription.Subtopology::nodes)
            .filterIsInstance<TopologyDescription.Processor>()
            .single { it.successors().isEmpty() }

        assertEquals("metrics-${table.stateStoreName}", lastProcessorNode.name())
    }
}

private fun inputTopic(testDriver: TopologyTestDriver, sourceTopic: Topic<String>) =
    testDriver.createInputTopic(
        sourceTopic.name,
        sourceTopic.keySerde.serializer(),
        sourceTopic.valueSerde.serializer()
    )