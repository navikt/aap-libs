package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.v2.processor.KMetadata
import no.nav.aap.kafka.streams.v2.processor.KProcessor
import no.nav.aap.kafka.streams.v2.processor.KStoreProcessor
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class ConsumeTest {
    @Test
    fun `consume and produce to a topic`() {
        val topology = topology {
            consume(Topics.A).produce(Topics.C)
            consume(Topics.B).produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("1", "a")
        kafka.inputTopic(Topics.B).produce("2", "b")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals("a", result["1"])
        assertEquals("b", result["2"])
        assertEquals(2, result.size)

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `consume and use custom processor`() {
        val topology = topology {
            consume(Topics.A)
                .processor(CustomProcessor())
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("1", "a")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("a.v2", result["1"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.build()))
    }

    @Test
    fun `consume and use custom processor with table`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .processor(CustomProcessorWithTable(table))
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", ".v2")
        kafka.inputTopic(Topics.A).produce("1", "a")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("a.v2", result["1"])

        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.build()))
    }
}

class CustomProcessorWithTable(table: KTable<String>) : KStoreProcessor<String, String>("custom-join", table) {
    override fun process(
        metadata: KMetadata,
        store: TimestampedKeyValueStore<String, String>,
        keyValue: KeyValue<String, String>
    ): String = "${keyValue.value}${store[keyValue.key].value()}"
}

open class CustomProcessor : KProcessor<String, String>("add-v2-prefix") {
    override fun process(metadata: KMetadata, keyValue: KeyValue<String, String>): String = "${keyValue.value}.v2"
}
