package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.StreamsMock
import no.nav.aap.kafka.streams.v2.Topics
import no.nav.aap.kafka.streams.v2.ms
import no.nav.aap.kafka.streams.v2.produce
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class WindowedStreamTest {

    @Test
    fun `reduce with sliding windows`() {
        val kafka = StreamsMock.withTopology {
            consume(Topics.A)
                .slidingWindow(100.ms)
                .reduce { s, s2 -> "$s$s2" }
                .produce(Topics.B)
        }

        println(kafka.visulize().uml())
        println(kafka.visulize().mermaid().generateDiagram())

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("1", "b")
            .produce("1", "c")

        val result = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("abc", result["1"])
    }


    @Test
    fun `reduce with hopping windows`() {
        val kafka = StreamsMock.withTopology {
            consume(Topics.A)
                .hoppingWindow(100.ms, advanceSize = 50.ms)
                .reduce { s, s2 -> "$s$s2" }
                .produce(Topics.B)
        }

        println(kafka.visulize().uml())
        println(kafka.visulize().mermaid().generateDiagram())

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("1", "b")
            .produce("1", "c")

        val result = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("abc", result["1"])
    }

    @Test
    fun `reduce with tumbling windows`() {
        val kafka = StreamsMock.withTopology {
            consume(Topics.A)
                .tumblingWindow(100.ms)
                .reduce { s, s2 -> "$s$s2" }
                .produce(Topics.B)
        }

        println(kafka.visulize().uml())
        println(kafka.visulize().mermaid().generateDiagram())

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("1", "b")
            .produce("1", "c")

        val result = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("abc", result["1"])
    }

    @Test
    fun `reduce with session windows`() {
        val kafka = StreamsMock.withTopology {
            consume(Topics.A)
                .sessionWindow(50.ms)
                .reduce { s, s2 -> "$s$s2" }
                .produce(Topics.B)
        }

        println(kafka.visulize().uml())
        println(kafka.visulize().mermaid().generateDiagram())

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("1", "b")
            .produce("1", "c")

        val result = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("abc", result["1"])
    }
}
