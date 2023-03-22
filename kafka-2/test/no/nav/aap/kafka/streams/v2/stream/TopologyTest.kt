package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topics
import no.nav.aap.kafka.streams.v2.kafkaWithTopology
import no.nav.aap.kafka.streams.v2.produce
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class TopologyTest {

    @Test
    fun `consume again`() {
        val kafka = kafkaWithTopology {
            consume(Topics.A).produce(Topics.B)
            consumeAgain(Topics.A).produce(Topics.C)
        }

        kafka.inputTopic(Topics.A).produce("1", "hello")

        val resultB = kafka.outputTopic(Topics.B).readKeyValuesToMap()
        val resultC = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, resultB.size)
        assertEquals("hello", resultB["1"])
        assertEquals(1, resultC.size)
        assertEquals("hello", resultC["1"])
    }
}