package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.Ignore
import kotlin.test.assertEquals
import kotlin.test.assertNull

class KafkaStreamsTest {

    private object Topics {
        val A = Topic("A", JsonSerde.jackson<String>())
        val B = Topic("B", JsonSerde.jackson<String>())
        val C = Topic("C", JsonSerde.jackson<String>())
    }

    private object Tables {
        val B = Table("table", Topics.B)
    }

    @Test
    fun `consume and produce to a topic`() {
        val kafka = topology {
            consume(Topics.A).produce(Topics.C)
            consume(Topics.B).produce(Topics.C)
        }.let(::topologyTestDriver)

        kafka.inputTopic(Topics.A).produce("1", "a")
        kafka.inputTopic(Topics.B).produce("2", "b")
        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals("a", result["1"])
        assertEquals("b", result["2"])
        assertEquals(2, result.size)
    }

    @Test
    fun `filter consumed topic`() {
        val kafka = topology {
            consume(Topics.A)
                .filter { it != "b" }
                .produce(Topics.C)
        }.let(::topologyTestDriver)

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("2", "b")
            .produce("3", "c")
        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertNull(result["2"])
    }

    @Test
    fun `rekey consumed topic`() {
        val kafka = topology {
            consume(Topics.A)
                .rekey { "test:$it" }
                .produce(Topics.C)
        }.let(::topologyTestDriver)

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("2", "b")
        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(2, result.size)
        assertEquals("a", result["test:a"])
        assertEquals("b", result["test:b"])
    }

    @Nested
    inner class Join {
        @Test
        fun `join topic with table`() {
            val kafka = topology {
                val table = consume(Topics.B).produce(Tables.B)
                consume(Topics.A)
                    .joinWith(table)
                    .map { a, b -> b + a }
                    .produce(Topics.C)
            }.let(::topologyTestDriver)

            val key = "1"
            kafka.inputTopic(Topics.B).produce(key, "B")
            kafka.inputTopic(Topics.A).produce(key, "A")
                .produce("2", "A") // shoule be skipped

            val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

            assertEquals(1, result.size)
            assertEquals("BA", result[key])
            assertNull(result["2"])
        }

        @Test
        fun `left join topic with table`() {
            val kafka = topology {
                val table = consume(Topics.B).produce(Tables.B)
                consume(Topics.A)
                    .leftJoinWith(table)
                    .map { left, _ -> left }
                    .produce(Topics.C)
            }.let(::topologyTestDriver)

            kafka.inputTopic(Topics.B).produce("1", "B")
            kafka.inputTopic(Topics.A).produce("1", "A")

            val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()
            assertEquals(1, result.size)
            assertEquals("A", result["1"])
        }

        @Test
        @Ignore // todo
        fun `left join topic with table with no match`() {
            val kafka = topology {
                val table = consume(Topics.B).produce(Tables.B)
                consume(Topics.A)
                    .leftJoinWith(table)
                    .map { left, _ -> left }
                    .produce(Topics.C)
            }.let(::topologyTestDriver)

//            kafka.inputTopic(Topics.B).produce("1", "B")
            kafka.inputTopic(Topics.A).produce("1", "A")

            val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()
            assertEquals(1, result.size)
            assertEquals("A", result["1"])
        }
    }

    @Nested
    inner class JoinFiltered {
        @Test
        fun `join filtered topic with table`() {
            val kafka = topology {
                val table = consume(Topics.B).produce(Tables.B)
                consume(Topics.A)
                    .filter { it != "humbug" }
                    .joinWith(table)
                    .map { a, b -> b + a }
                    .produce(Topics.C)
            }.let(::topologyTestDriver)

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
        }

        @Test
        fun `left join filtered topic with table`() {

        }
    }

    @Nested
    inner class Map {
        @Test
        fun `map a joined stream`() {
            val kafka = topology {
                val table = consume(Topics.B).produce(Tables.B)
                consume(Topics.A)
                    .joinWith(table)
                    .map { a, b -> b + a }
                    .produce(Topics.C)
            }.let(::topologyTestDriver)

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
        }

        @Test
        fun `map a left joined stream`() {

        }
    }

    @Nested
    inner class MapFiltered {
        @Test
        fun `map a filtered joined stream`() {
            val kafka = topology {
                val table = consume(Topics.B).produce(Tables.B)
                consume(Topics.A)
                    .joinWith(table)
                    .filter { (a, _) -> a == "sauce" }
                    .map { a, b -> b + a }
                    .produce(Topics.C)
            }.let(::topologyTestDriver)

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
        }

        @Test
        fun `map a filtered left joined stream`() {

        }
    }

    @Nested
    inner class FilterMapped {
        @Test
        fun `filter a mapped joined stream`() {
            val kafka = topology {
                val table = consume(Topics.B).produce(Tables.B)
                consume(Topics.A)
                    .joinWith(table)
                    .map { a, b -> b + a }
                    .filter { it == "niceprice" }
                    .produce(Topics.C)
            }.let(::topologyTestDriver)

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
        }

        @Test
        fun `filter a mapped left joined stream`() {

        }
    }

    private fun <V> TopologyTestDriver.inputTopic(topic: Topic<V>): TestInputTopic<String, V> =
        createInputTopic(topic.name, topic.keySerde.serializer(), topic.valueSerde.serializer())

    private fun <V> TopologyTestDriver.outputTopic(topic: Topic<V>): TestOutputTopic<String, V> =
        createOutputTopic(topic.name, topic.keySerde.deserializer(), topic.valueSerde.deserializer())

    private fun <V> TestInputTopic<String, V>.produce(key: String, value: V): TestInputTopic<String, V> =
        pipeInput(key, value).let { this }

    private fun topologyTestDriver(topology: Topology) = TopologyTestDriver(topology.build())
}