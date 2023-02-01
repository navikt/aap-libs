package no.nav.aap.kafka.streams.v2

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class RekeyTest {
    @Test
    fun `rekey consumed topic`() {
        val topology = topology {
            consume(Topics.A)
                .rekey { "test:$it" }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("2", "b")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertEquals("a", result["test:a"])
        assertEquals("b", result["test:b"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `rekey with mapKeyValue`() {
        val topology = topology {
            consume(Topics.A)
                .mapKeyValue{ key, value -> KeyValue(key = "test:$key", value = "$value$value")}
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("2", "b")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertEquals("aa", result["test:1"])
        assertEquals("bb", result["test:2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }
}
