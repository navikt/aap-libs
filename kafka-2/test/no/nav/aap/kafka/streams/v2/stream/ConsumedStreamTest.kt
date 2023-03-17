package no.nav.aap.kafka.streams.v2.stream

import net.logstash.logback.argument.StructuredArguments
import no.nav.aap.kafka.streams.v2.*
import no.nav.aap.kafka.streams.v2.Tables
import no.nav.aap.kafka.streams.v2.Topics
import no.nav.aap.kafka.streams.v2.kafka
import no.nav.aap.kafka.streams.v2.kafkaWithTopology
import no.nav.aap.kafka.streams.v2.processor.Processor
import no.nav.aap.kafka.streams.v2.processor.ProcessorMetadata
import no.nav.aap.kafka.streams.v2.produce
import no.nav.aap.kafka.streams.v2.produceTombstone
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class ConsumedStreamTest {

    @Test
    fun `map with metadata`() {
        val kafka = kafkaWithTopology {
            consume(Topics.A)
                .mapWithMetadata { value, metadata -> value + metadata.topic }
                .produce(Topics.B)
        }

        kafka.inputTopic(Topics.A).produce("1", "hello:")

        val result = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("hello:A", result["1"])
    }

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

//        println(kafka.visulize().mermaid())
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `consume and use custom processor`() {
        val topology = topology {
            consume(Topics.A)
                .processor(CustomProcessor())
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("1", "a")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("a.v2", result["1"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.build()))
    }

    @Test
    fun `consume and use custom processor with table`() {
        val topology = topology {
            val table = consume(Tables.B)
            consume(Topics.A)
                .processor(CustomProcessorWithTable(table))
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", ".v2")
        kafka.inputTopic(Topics.A).produce("1", "a")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("a.v2", result["1"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.build()))
    }

    @Test
    fun `consume and use custom processor with mapping`() {
        val topology = topology {
            consume(Topics.A)
                .processor(object: Processor<String, Int>("named") {
                    override fun process(metadata: ProcessorMetadata, keyValue: KeyValue<String, String>): Int {
                        return keyValue.value.toInt() + 1
                    }
                })
                .map(Int::toString)
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("a", "1")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("2", result["a"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.build()))
    }

    @Test
    fun `consume and use custom processor in place`() {
        val topology = topology {
            consume(Topics.A)
                .processor(object: Processor<String, String>("something") {
                    override fun process(metadata: ProcessorMetadata, keyValue: KeyValue<String, String>): String {
                        return keyValue.value
                    }
                })
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("a", "1")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("1", result["a"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.build()))
    }

    @Test
    fun `consume on each`() {
        val result = mutableListOf<Int>()
        val topology = topology {
            consume(Topics.A) { _, value, _ ->
                result.add(value?.length ?: -1)
            }
        }

        val kafka = kafka(topology)
        kafka.inputTopic(Topics.A).produce("1", "something").produceTombstone("2")

        assertEquals(result, mutableListOf(9, -1))
    }

    @Test
    fun `consume and secureLog`() {
        val topology = topology {
            consume(Topics.A)
                .secureLog {
                    info("test message $it")
                }
        }

        val kafka = kafka(topology)
        kafka.inputTopic(Topics.A).produce("1", "something")
    }

    @Test
    fun `consume and secureLog with structured arguments`() {
        val topology = topology {
            consume(Topics.A)
                .secureLog {
                    info("test message $it", StructuredArguments.kv("test", "hey"))
                }
        }

        val kafka = kafka(topology)
        kafka.inputTopic(Topics.A).produce("1", "something")
    }

    @Test
    fun `consume and secureLogWithKey`() {
        val topology = topology {
            consume(Topics.A)
                .secureLogWithKey { key, value ->
                    info("test message $key $value")
                }
        }

        val kafka = kafka(topology)
        kafka.inputTopic(Topics.A).produce("1", "something")
    }

    @Test
    fun `filter key`() {
        val topology = topology {
            consume(Topics.A)
                .filterKey { it == "3" }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A)
            .produce("1", "a")
            .produce("3", "b")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("b", result["3"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }
}

