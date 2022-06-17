package no.nav.aap.kafka.streams

import no.nav.aap.kafka.serde.json.JsonSerde
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.errors.StreamsException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class JsonSerdeTest {

    data class Dto(val version: Int = 2, val title: String)
    data class DtoPrevious(val version: Int = 1)
    data class DtoPreviousWithoutVersion(val title: String)
    data class DtoUnsupported(val version: Int = 2, val error: String)

    private fun migrationScript(previous: DtoPrevious) = Dto(previous.version + 1, "unknown")
    private fun migrateUnversioned(previous: DtoPreviousWithoutVersion) = Dto(title = previous.title)

    @Test
    fun `can deserialize new version`() {
        val sourceTopic = Topic("source", JsonSerde.jackson(2, ::migrationScript))
        val sinkTopic = Topic("sink", JsonSerde.jackson<Dto>())

        val topology = StreamsBuilder().apply {
            consume(sourceTopic)
                .filterNotNull("skip")
                .produce(sinkTopic, "sink")
        }.build()

        val kafka = TopologyTestDriver(topology)
        val sinkOutputTopic = kafka.outputTopic(sinkTopic)
        val sourceInputTopic = kafka.inputTopic(sourceTopic)
        sourceInputTopic.pipeInput("123", Dto(title = "nice"))

        val result = sinkOutputTopic.readKeyValuesToList().single()
        assertEquals("123", result.key)
        assertEquals(2, result.value.version)
        assertEquals("nice", result.value.title)
    }

    @Test
    fun `can deserialize previous version`() {
        val sourceTopic = Topic("source", JsonSerde.jackson(2, ::migrationScript))
        val sinkTopic = Topic("sink", JsonSerde.jackson<Dto>())

        val topology = StreamsBuilder().apply {
            consume(sourceTopic)
                .filterNotNull("skip")
                .produce(sinkTopic, "sink")
        }.build()

        val kafka = TopologyTestDriver(topology)
        val sourceInputTopic = kafka.inputTopic(sourceTopic, JsonSerde.jackson<DtoPrevious>().serializer())
        val sinkOutputTopic = kafka.outputTopic(sinkTopic)
        sourceInputTopic.pipeInput("123", DtoPrevious())

        val result = sinkOutputTopic.readKeyValuesToList().single()
        assertEquals("123", result.key)
        assertEquals(2, result.value.version)
    }

    @Test
    fun `can use both versions`() {
        val sourceTopic = Topic("source", JsonSerde.jackson(2, ::migrationScript))
        val sinkTopic = Topic("sink", JsonSerde.jackson<Dto>())

        val topology = StreamsBuilder().apply {
            consume(sourceTopic)
                .filterNotNull("skip")
                .produce(sinkTopic, "sink")
        }.build()

        val kafka = TopologyTestDriver(topology)
        val testInputV1Topic = kafka.inputTopic(sourceTopic, JsonSerde.jackson<DtoPrevious>().serializer())
        val testInputV2Topic = kafka.inputTopic(sourceTopic)
        val sinkOutputTopic = kafka.outputTopic(sinkTopic)

        testInputV1Topic.pipeInput("111", DtoPrevious())
        testInputV2Topic.pipeInput("222", Dto(title = "to to to"))
        testInputV2Topic.pipeInput("333", Dto(title = "tre tre tre"))
        testInputV1Topic.pipeInput("444", DtoPrevious())

        val result111 = sinkOutputTopic.readKeyValue()
        assertEquals("111", result111.key)
        assertEquals(Dto(title = "unknown"), result111.value)
        assertEquals(2, result111.value.version)

        val result222 = sinkOutputTopic.readKeyValue()
        assertEquals("222", result222.key)
        assertEquals(Dto(title = "to to to"), result222.value)
        assertEquals(2, result111.value.version)

        val result333 = sinkOutputTopic.readKeyValue()
        assertEquals("333", result333.key)
        assertEquals(Dto(title = "tre tre tre"), result333.value)
        assertEquals(2, result333.value.version)

        val result444 = sinkOutputTopic.readKeyValue()
        assertEquals("444", result444.key)
        assertEquals(Dto(title = "unknown"), result444.value)
        assertEquals(2, result444.value.version)

        assertTrue(sinkOutputTopic.isEmpty)
    }

    @Test
    fun `cannot deserialize unsupported migration`() {
        val sourceTopic = Topic("source", JsonSerde.jackson(2, ::migrationScript))
        val sinkTopic = Topic("sink", JsonSerde.jackson<Dto>())

        val topology = StreamsBuilder().apply {
            consume(sourceTopic)
                .filterNotNull("skip")
                .produce(sinkTopic, "sink")
        }.build()

        val kafka = TopologyTestDriver(topology)
        val unsupportedInputTopic = kafka.inputTopic(sourceTopic, JsonSerde.jackson<DtoUnsupported>().serializer())

        kafka.outputTopic(sinkTopic)

        assertThrows<StreamsException> {
            unsupportedInputTopic.pipeInput("123", DtoUnsupported(error = "oh no"))
        }
    }

    @Test
    fun `cannot deserialize previous unversioned dto`() {
        val sourceTopic = Topic("source", JsonSerde.jackson(2, ::migrateUnversioned))
        val sinkTopic = Topic("sink", JsonSerde.jackson<Dto>())

        val topology = StreamsBuilder().apply {
            consume(sourceTopic)
                .filterNotNull("skip")
                .produce(sinkTopic, "sink")
        }.build()

        val kafka = TopologyTestDriver(topology)

        data class NotPreviousUnversionedDto(val flag: Boolean)

        val notPreviousUnversionedInputTopic =
            kafka.inputTopic(sourceTopic, JsonSerde.jackson<NotPreviousUnversionedDto>().serializer())

        kafka.outputTopic(sinkTopic)

        assertThrows<StreamsException> {
            notPreviousUnversionedInputTopic.pipeInput("123", NotPreviousUnversionedDto(true))
        }
    }
}

private fun <V, T> TopologyTestDriver.inputTopic(topic: Topic<V>, serializer: Serializer<T>) =
    createInputTopic(topic.name, topic.keySerde.serializer(), serializer)

private fun <V> TopologyTestDriver.inputTopic(topic: Topic<V>) =
    createInputTopic(topic.name, topic.keySerde.serializer(), topic.valueSerde.serializer())

private fun <V> TopologyTestDriver.outputTopic(topic: Topic<V>) =
    createOutputTopic(topic.name, topic.keySerde.deserializer(), topic.valueSerde.deserializer())
