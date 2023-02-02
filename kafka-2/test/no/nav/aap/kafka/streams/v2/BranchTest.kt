package no.nav.aap.kafka.streams.v2

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class BranchTest {

    @Test
    fun `consume and branch stream`() {
        val topology = topology {
            consume(Topics.A)
                .branch({ v -> v == "lol" }, {
                    it.produce(Topics.C)
                })
                .branch({ v -> v != "lol" }, {
                    it.produce(Topics.B)
                })
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("1", "lol")
        kafka.inputTopic(Topics.A).produce("2", "ikke lol")

        val resultC = kafka.outputTopic(Topics.C).readKeyValuesToMap()
        val resultB = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals("lol", resultC["1"])
        assertEquals("ikke lol", resultB["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }
}
