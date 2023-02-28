package no.nav.aap.kafka.streams.v2

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class MermaidTest {

    @Test
    fun `join i en stream og initier i en annen`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .joinWith(table)
                .map { l, r -> r + l }
                .produce(Topics.C)

            consume(Topics.D)
                .produce(Topics.A)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", "hello")
        kafka.inputTopic(Topics.D).produce("1", " på do")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals("hello på do", result["1"])

        println(kafka.visulize().mermaid().generateDiagram())
    }

    @Test
    fun `include custom topic to db`() {
        val topology = topology {
            consume(Topics.A)
            consume(Topics.B)
            consume(Topics.C)
        }

        val kafka = kafka(topology)

        println(kafka.visulize().mermaid().generateDiagramWithDatabaseSink(topicToDb = mapOf(
            Topics.A.name to "postgres",
            Topics.B.name to "postgres",
            Topics.C.name to "postgres",
        )))
    }

    @Test
    fun `custom state processor`() {
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

        println(kafka.visulize().mermaid().generateDiagram())
    }
}