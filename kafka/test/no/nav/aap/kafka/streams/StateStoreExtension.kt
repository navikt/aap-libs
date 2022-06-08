package no.nav.aap.kafka.streams

import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.store.allValues
import no.nav.aap.kafka.streams.store.scheduleCleanup
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TopologyDescription
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

internal class StateStoreExtension {

    @Test
    fun `read all values from read only state store`() {
        val topic = Topic("source", JsonSerde.jackson<String>())
        val table = Table("table", topic)

        val topology = StreamsBuilder().apply {
            consume(topic).filterNotNull("skip-tombstone").produce(table)
        }.build()

        val kafka = TopologyTestDriver(topology)
        inputTopic(kafka, topic).pipeInput("123", "hello")
        inputTopic(kafka, topic).pipeInput("456", "hello")

        val stateStore = kafka.getKeyValueStore<String, String>("${table.name}-state-store")
        val allValues = stateStore.allValues()
        assertEquals(2, allValues.size)
        assertEquals("hello", allValues.distinctBy { it }.firstOrNull())
    }

    @Test
    fun `state store cleaner removes requested`() {
        val topic = Topic("source", JsonSerde.jackson<String>())
        val table = Table("table", topic)
        val keysToDelete = mutableListOf<String>()

        val stream = StreamsBuilder()
        val ktable = stream.consume(topic).filterNotNull("skip-tombstone").produce(table)
        ktable.scheduleCleanup(table.stateStoreName, keysToDelete)
        val topology = stream.build()

        val kafka = TopologyTestDriver(topology)
        val inputTopic = inputTopic(kafka, topic)
        inputTopic.pipeInput("123", "hello")
        inputTopic.pipeInput("456", "hello")

        val stateStore = kafka.getKeyValueStore<String, String>("${table.name}-state-store")
        assertEquals(2, stateStore.allValues().size)

        keysToDelete.add("123")
        kafka.advanceWallClockTime(Duration.ofSeconds(1)) // processor api punctuate 1 sec
        assertEquals(1, stateStore.allValues().size)
        assertEquals("hello", stateStore["456"])
    }

    @Test
    fun `state store cleaner is named`() {
        val topic = Topic("source", JsonSerde.jackson<String>())
        val table = Table("table", topic)
        val keysToDelete = mutableListOf<String>()

        val stream = StreamsBuilder()
        val ktable = stream.consume(topic).filterNotNull("skip-tombstone").produce(table)
        ktable.scheduleCleanup(table.stateStoreName, keysToDelete) // last defined processor node
        val topology = stream.build()

        val kafka = TopologyTestDriver(topology)
        inputTopic(kafka, topic)

        val lastProcessorNode = topology
            .describe()
            .subtopologies()
            .flatMap(TopologyDescription.Subtopology::nodes)
            .filterIsInstance<TopologyDescription.Processor>()
            .single { it.successors().isEmpty() }

        assertEquals("cleanup-${table.stateStoreName}", lastProcessorNode.name())
    }
}

private fun inputTopic(testDriver: TopologyTestDriver, sourceTopic: Topic<String>) =
    testDriver.createInputTopic(
        sourceTopic.name,
        sourceTopic.keySerde.serializer(),
        sourceTopic.valueSerde.serializer()
    )