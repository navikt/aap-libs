package no.nav.aap.kafka.streams.v2

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

 class ConsumeTest {
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
}
