package no.nav.aap.kafka.streams.v2

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class MapTest {
    @Test
    fun `map a filtered joined stream`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .joinWith(table)
                .filter { (a, _) -> a == "sauce" }
                .map { a, b -> b + a }
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
        assertEquals("awesomesauce", result["1"])
        assertNull(result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `map a filtered left joined stream`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(table)
                .filter { (a, _) -> a == "sauce" }
                .map { a, b -> b + a }
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
        assertEquals("awesomesauce", result["1"])
        assertNull(result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `map a joined stream`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .joinWith(table)
                .map { a, b -> b + a }
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

        assertEquals(2, result.size)
        assertEquals("awesomesauce", result["1"])
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `map a left joined stream`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(table)
                .map { a, b -> b + a }
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

        assertEquals(2, result.size)
        assertEquals("awesomesauce", result["1"])
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `map key and value`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(table)
                .mapKeyValue { key, left, right -> KeyValue("$key$key", right + left) }
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

        assertEquals(2, result.size)
        assertEquals("awesomesauce", result["11"])
        assertEquals("niceprice", result["22"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }
}
