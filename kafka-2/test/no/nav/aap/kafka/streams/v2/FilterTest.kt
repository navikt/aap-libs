package no.nav.aap.kafka.streams.v2

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class FilterTest {

    @Test
    fun `filter consumed topic`() {
        val topology = topology {
            consume(Topics.A)
                .filter { it != "b" }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("2", "b")
            .produce("3", "c")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertNull(result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `filter a mapped joined stream`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .joinWith(table)
                .map { a, b -> b + a }
                .filter { it == "niceprice" }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B)
            .produce("1", "awesome")
            .produce("2", "nice")

        kafka.inputTopic(Topics.A)
            .produce("1", "sauce")
            .produce("2", "price")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertNull(result["1"])
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `filter a mapped left joined stream`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(table)
                .map { a, b -> b + a }
                .filter { it == "niceprice" }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B)
            .produce("1", "awesome")
            .produce("2", "nice")

        kafka.inputTopic(Topics.A)
            .produce("1", "sauce")
            .produce("2", "price")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertNull(result["1"])
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `filter a filtered mapped stream`() {
        val topology = topology {
            consume(Topics.A)
                .filter { it.contains("nice") }
                .filter { it.contains("price") }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A)
            .produce("1", "awesomenice")
            .produce("2", "niceprice")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `filter a filtered left joined stream`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(table)
                .map { a, b -> b + a }
                .filter { it.contains("nice") }
                .filter { it.contains("price") }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B)
            .produce("1", "awesome")
            .produce("2", "nice")

        kafka.inputTopic(Topics.A)
            .produce("1", "sauce")
            .produce("2", "price")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertNull(result["1"])
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }
}
