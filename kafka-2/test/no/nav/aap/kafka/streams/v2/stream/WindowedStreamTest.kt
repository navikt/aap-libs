package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topics
import no.nav.aap.kafka.streams.v2.kafka
import no.nav.aap.kafka.streams.v2.produce
import no.nav.aap.kafka.streams.v2.topology
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.time.DurationUnit
import kotlin.time.toDuration

internal class WindowedStreamTest {

    private val Int.ms get() = toDuration(DurationUnit.MILLISECONDS)

    @Test
    fun tes() {
        val topology = topology {
            consume(Topics.A, true)
                .windowed(100.ms, 100.ms)
                .reduce { s, s2 -> "$s$s2" }
                .produce(Topics.B, true)
        }

        val kafka = kafka(topology)
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
