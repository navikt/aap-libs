package no.nav.aap.kafka.streams.v2

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class JoinTest {

    @Test
    fun `join topic with table`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .joinWith(table)
                .map { a, b -> b + a }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        val key = "1"
        kafka.inputTopic(Topics.B).produce(key, "B")
        kafka.inputTopic(Topics.A).produce(key, "A")
            .produce("2", "A") // shoule be skipped

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("BA", result[key])
        assertNull(result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `join filtered topic with table`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .filter { it != "humbug" }
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
            .produce("1", "humbug")
            .produce("2", "humbug")
            .produce("2", "price")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertEquals("awesomesauce", result["1"])
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `join topic with table and write back to topic`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(table)
                .map { a, b -> a + b }
                .produce(Topics.B)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B)
            .produce("1", "sauce")
            .produce("2", "price")

        kafka.inputTopic(Topics.A)
            .produce("1", "awesome")
            .produce("2", "nice")

        val result = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertEquals("awesomesauce", result["1"])
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `left join topic with table`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(table)
                .map { left, _ -> left }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", "B")
        kafka.inputTopic(Topics.A).produce("1", "A")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("A", result["1"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `left join topic with table with no match`() {
        val topology = topology {
            val tableB = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(tableB)
                .map { left, right -> right ?: left }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("1", "A")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("A", result["1"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `left join filtered topic with table`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .filter { it != "humbug" }
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
            .produce("1", "humbug")
            .produce("2", "humbug")
            .produce("2", "price")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertEquals("awesomesauce", result["1"])
        assertEquals("niceprice", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `left join filtered topic with empty table is not filtered out`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .filter { it != "humbug" }
                .leftJoinWith(table)
                .map { a, b -> (b ?: "") + a }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A)
            .produce("1", "sauce")
            .produce("1", "humbug")
            .produce("2", "humbug")
            .produce("2", "price")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertEquals("sauce", result["1"])
        assertEquals("price", result["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }
}
