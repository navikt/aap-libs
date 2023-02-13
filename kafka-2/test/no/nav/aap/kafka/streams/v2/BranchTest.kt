package no.nav.aap.kafka.streams.v2

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class BranchTest {

    @Test
    fun `branch stream from consumed`() {
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

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `default branch from consumed`() {
        val topology = topology {
            consume(Topics.A)
                .branch({ v -> v == "lol" }, {
                    it.produce(Topics.C)
                })
                .default {
                    it.produce(Topics.B)
                }
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("1", "lol")
        kafka.inputTopic(Topics.A).produce("2", "ikke lol")

        val resultC = kafka.outputTopic(Topics.C).readKeyValuesToMap()
        val resultB = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals("lol", resultC["1"])
        assertEquals("ikke lol", resultB["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `branch stream from mapped`() {
        val topology = topology {
            consume(Topics.A)
                .map { i -> i }
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

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.build()))
    }

    @Test
    fun `branch en branched stream from mapped`() {
        val topology = topology {
            consume(Topics.A)
                .map { i -> i }
                .branch({ v -> v == "lol" }, {
                    it
                        .branch({ true }) { b -> b.produce(Topics.C) }
                        .branch({ false }) { b -> b.produce(Topics.B) }
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

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.buildInternalTopology()))
    }

    @Test
    fun `default branch stream from mapped`() {
        val topology = topology {
            consume(Topics.A)
                .map { i -> i }
                .branch({ v -> v == "lol" }, {
                    it.produce(Topics.C)
                })
                .default {
                    it.produce(Topics.B)
                }
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("1", "lol")
        kafka.inputTopic(Topics.A).produce("2", "ikke lol")

        val resultC = kafka.outputTopic(Topics.C).readKeyValuesToMap()
        val resultB = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals("lol", resultC["1"])
        assertEquals("ikke lol", resultB["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `branch stream from joined stream`() {
        val topology = topology {
            val tableB = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .joinWith(tableB)
                .branch({ (left, _) -> left == "lol" }, {

                    it.map { (left, right) -> left + right }
                        .produce(Topics.C)

                })
                .branch({ (_, right) -> right == "lol" }, {

                    it.map { (_, right) -> right + right }
                        .produce(Topics.D)

                })
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", "lol") // right
        kafka.inputTopic(Topics.B).produce("2", "lol") // right
        kafka.inputTopic(Topics.A).produce("1", "lol") // left
        kafka.inputTopic(Topics.A).produce("2", "ikke lol")

        val resultC = kafka.outputTopic(Topics.C).readKeyValuesToMap()
        val resultD = kafka.outputTopic(Topics.D).readKeyValuesToMap()

        assertEquals("lollol", resultC["1"])
        assertEquals("lollol", resultD["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
    }

    @Test
    fun `default branch from joined stream`() {
        val topology = topology {
            val tableB = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .joinWith(tableB)
                .branch({ (left, _) -> left == "lol" }, {
                    it.map { (left, right) -> left + right }.produce(Topics.C)

                })
                .default {
                    it.map { (_, right) -> right + right }.produce(Topics.D)
                }
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", "lol") // right
        kafka.inputTopic(Topics.B).produce("2", "lol") // right
        kafka.inputTopic(Topics.A).produce("1", "lol") // left
        kafka.inputTopic(Topics.A).produce("2", "ikke lol")

        val resultC = kafka.outputTopic(Topics.C).readKeyValuesToMap()
        val resultD = kafka.outputTopic(Topics.D).readKeyValuesToMap()

        assertEquals("lollol", resultC["1"])
        assertEquals("lollol", resultD["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `branch stream from left joined stream`() {
        val topology = topology {
            val tableB = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(tableB)
                .branch({ (left, _) -> left == "lol" }, {

                    it.map { (left, right) -> left + right }
                        .produce(Topics.C)

                })
                .branch({ (_, right) -> right == "lol" }, {

                    it.map { (_, right) -> right + right }
                        .produce(Topics.D)

                })
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", "lol") // right
        kafka.inputTopic(Topics.B).produce("2", "lol") // right
        kafka.inputTopic(Topics.A).produce("1", "lol") // left
        kafka.inputTopic(Topics.A).produce("2", "ikke lol")

        val resultC = kafka.outputTopic(Topics.C).readKeyValuesToMap()
        val resultD = kafka.outputTopic(Topics.D).readKeyValuesToMap()

        assertEquals("lollol", resultC["1"])
        assertEquals("lollol", resultD["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `default branch from left joined stream`() {
        val topology = topology {
            val tableB = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .leftJoinWith(tableB)
                .branch({ (left, _) -> left == "lol" }, {
                    it.map { (left, right) -> left + right }.produce(Topics.C)

                })
                .default {
                    it.map { (_, right) -> right + right }.produce(Topics.D)
                }
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", "lol") // right
        kafka.inputTopic(Topics.B).produce("2", "lol") // right
        kafka.inputTopic(Topics.A).produce("1", "lol") // left
        kafka.inputTopic(Topics.A).produce("2", "ikke lol")

        val resultC = kafka.outputTopic(Topics.C).readKeyValuesToMap()
        val resultD = kafka.outputTopic(Topics.D).readKeyValuesToMap()

        assertEquals("lollol", resultC["1"])
        assertEquals("lollol", resultD["2"])

//        println(no.nav.aap.kafka.streams.v2.visual.Mermaid.generate("test", topology))
//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }
}
