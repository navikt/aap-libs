package no.nav.aap.kafka.streams

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import no.nav.aap.kafka.SecureLogAppender
import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.structuredArguments
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Named
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.test.assertEquals

internal class KafkaStreamsExtensionTest {

    @Nested
    inner class SourceStream {

        @Test
        fun `can consume topic`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val topology = StreamsBuilder().apply { consume(sourceTopic) }.build()
            inputTopic(TopologyTestDriver(topology), sourceTopic).pipeInput("123", "hello")
        }

        @Test
        fun `consumed topic is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val topology = StreamsBuilder().apply { consume(sourceTopic) }.build()
            inputTopic(TopologyTestDriver(topology), sourceTopic).pipeInput("123", "hello")

            assertEquals("consume-source", topology.singleSourceName)
        }

        @Test
        fun `consume topic is logged`() {
            val log = SecureLogAppender()
                .apply { context = LoggerFactory.getILoggerFactory() as LoggerContext }
                .also {
                    (LoggerFactory.getLogger("secureLog") as Logger).apply {
                        level = Level.TRACE
                        addAppender(it)
                    }
                }
                .apply { start() }

            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val topology = StreamsBuilder().apply { consume(sourceTopic, true) }.build()
            inputTopic(TopologyTestDriver(topology), sourceTopic).pipeInput("123", "hello")

            val secureMsg = log.firstContaining("Konsumerer")
            val args = secureMsg.structuredArguments()

            assertEquals("Konsumerer Topic", secureMsg.message)
            assertEquals("123", args["key"])
            assertEquals("hello", args["value"])
            assertEquals("source", args["topic"])
            assertEquals("0", args["partition"])
            assertEquals("0", args["offset"])
        }

        @Test
        fun `consumed topic is logged with Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val topology = StreamsBuilder().apply { consume(sourceTopic) }.build()
            inputTopic(TopologyTestDriver(topology), sourceTopic).pipeInput("123", "hello")

            assertEquals("log-consume-source", topology.singleProcessorName)
        }
    }

    @Nested
    inner class ProcessorStream {

        @Test
        fun `can filterNotNull`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            val sinkOutputTopic = outputTopic(kafka, sinkTopic)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")

            val result = sinkOutputTopic.readKeyValuesToList()
            assertEquals(1, result.size)
            assertEquals("123", result.first().key)
            assertEquals("hello", result.first().value)
        }

        @Test
        fun `filterNotNull is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .produce(sinkTopic, "produce-sink")
            }.build()

            TopologyTestDriver(topology).apply {
                outputTopic(this, sinkTopic)
                inputTopic(this, sourceTopic).pipeInput("123", "hello")
            }

            assertEquals("filter-tombstone", topology.processorName("log-consume-source"))
        }

        @Test
        fun `can filter`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .filter("filter-hello") { _, v -> v == "hello" }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            val inputTopic = inputTopic(kafka, sourceTopic)
            val outputTopic = outputTopic(kafka, sinkTopic)

            inputTopic.pipeInput("123", "goodbye")
            inputTopic.pipeInput("456", "hello")

            val result = outputTopic.readKeyValuesToList()
            assertEquals(1, result.size)
            assertEquals("456", result.first().key)
            assertEquals("hello", result.first().value)
        }

        @Test
        fun `filter is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .filter("filter-hello") { _, v -> v == "hello" }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")
            outputTopic(kafka, sinkTopic)

            assertEquals("filter-hello", topology.processorName("filter-tombstone"))
        }

        @Test
        fun `can mapValues`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .mapValues("reversed") { _, v -> v.reversed() }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            val inputTopic = inputTopic(kafka, sourceTopic)
            val outputTopic = outputTopic(kafka, sinkTopic)

            inputTopic.pipeInput("123", "hello")

            val result = outputTopic.readKeyValuesToList()
            assertEquals(1, result.size)
            assertEquals("123", result.first().key)
            assertEquals("olleh", result.first().value)
        }

        @Test
        fun `mapValues is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .mapValues("reversed") { _, v -> v.reversed() }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")
            outputTopic(kafka, sinkTopic)

            assertEquals("reversed", topology.processorName("filter-tombstone"))
        }

        @Test
        fun `can flatMapValues`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .flatMapValues("flatten") { v -> v.split(" ") }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            val inputTopic = inputTopic(kafka, sourceTopic)
            val outputTopic = outputTopic(kafka, sinkTopic)

            inputTopic.pipeInput("123", "hello you")

            val result = outputTopic.readKeyValuesToList()
            assertEquals(2, result.filter { it.key == "123" }.size)
            assertEquals(listOf("hello", "you"), result.map { it.value })
        }

        @Test
        fun `can flatMapValues with key`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .flatMapValues("flatten") { _, v -> v.split(" ") }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            val inputTopic = inputTopic(kafka, sourceTopic)
            val outputTopic = outputTopic(kafka, sinkTopic)

            inputTopic.pipeInput("123", "hello you")

            val result = outputTopic.readKeyValuesToList()
            assertEquals(2, result.filter { it.key == "123" }.size)
            assertEquals(listOf("hello", "you"), result.map { it.value })
        }

        @Test
        fun `flatMapValues is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .flatMapValues("flatten") { v -> v.split(" ") }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")
            outputTopic(kafka, sinkTopic)

            assertEquals("flatten", topology.processorName("filter-tombstone"))
        }

        @Test
        fun `flatMapValues with key is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("filter-tombstone")
                    .flatMapValues("flatten") { _, v -> v.split(" ") }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")
            outputTopic(kafka, sinkTopic)

            assertEquals("flatten", topology.processorName("filter-tombstone"))
        }

        @Test
        fun `can mapNotNull`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .mapNotNull("not-null") { v -> v.reversed() }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            val inputTopic = inputTopic(kafka, sourceTopic)
            val outputTopic = outputTopic(kafka, sinkTopic)

            inputTopic.pipeInput("123", "hello")

            val result = outputTopic.readKeyValuesToList()
            assertEquals(1, result.size)
            assertEquals("123", result.first().key)
            assertEquals("olleh", result.first().value)
        }

        @Test
        fun `can mapNotNull with key`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .mapNotNull("not-null") { _, v -> v.reversed() }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            val inputTopic = inputTopic(kafka, sourceTopic)
            val outputTopic = outputTopic(kafka, sinkTopic)

            inputTopic.pipeInput("123", "hello")

            val result = outputTopic.readKeyValuesToList()
            assertEquals(1, result.size)
            assertEquals("123", result.first().key)
            assertEquals("olleh", result.first().value)
        }

        @Test
        fun `mapNotNull is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .mapNotNull("not-null") { v -> v.reversed() }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")
            outputTopic(kafka, sinkTopic)

            assertEquals("not-null-filter-premap", topology.processorName("log-consume-${sourceTopic.name}"))
            assertEquals("not-null-map", topology.processorName("not-null-filter-premap"))
            assertEquals("not-null-filter-postmap", topology.processorName("not-null-map"))
        }

        @Test
        fun `mapNotNull with key is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .mapNotNull("not-null") { _, v -> v.reversed() }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")
            outputTopic(kafka, sinkTopic)

            assertEquals("not-null-filter-premap", topology.processorName("log-consume-${sourceTopic.name}"))
            assertEquals("not-null-map", topology.processorName("not-null-filter-premap"))
            assertEquals("not-null-filter-postmap", topology.processorName("not-null-map"))
        }

        @Test
        fun `can selectKey`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("tombstone")
                    .selectKey("select-value-as-key") { _, v -> v }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")
            val outputTopic = outputTopic(kafka, sinkTopic)

            val result = outputTopic.readKeyValue()
            assertEquals("hello", result.key)
            assertEquals("hello", result.value)
        }

        @Test
        fun `selectKey is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .filterNotNull("tombstone")
                    .selectKey("select-value-as-key") { _, v -> v }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")
            outputTopic(kafka, sinkTopic)

            assertEquals("select-value-as-key", topology.processorName("tombstone"))
        }

        @Test
        fun `can join`() {
            val leftTopic = Topic("left", JsonSerde.jackson<String>())
            val rightTopic = Topic("right", JsonSerde.jackson<String>())
            val rightTable = Table("right", rightTopic)
            val bothTopic = Topic("both", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                val ktable = consume(rightTopic).produce(rightTable)

                consume(leftTopic)
                    .filterNotNull("skip-right-tombstone")
                    .join(leftTopic with rightTopic, ktable) { l, r -> "$l and $r" }
                    .produce(bothTopic, "produced-both")
            }.build()

            val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
            val kafka = TopologyTestDriver(topology, config)
            val outputTopic = outputTopic(kafka, bothTopic)
            inputTopic(kafka, rightTopic).pipeInput("123", "banana")
            inputTopic(kafka, leftTopic).pipeInput("123", "apple")

            val result = outputTopic.readKeyValue()
            assertEquals("123", result.key)
            assertEquals("apple and banana", result.value)
        }

        @Test
        fun `join is Named`() {
            val leftTopic = Topic("left", JsonSerde.jackson<String>())
            val rightTopic = Topic("right", JsonSerde.jackson<String>())
            val rightTable = Table("right", rightTopic)
            val bothTopic = Topic("both", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                val ktable = consume(rightTopic).produce(rightTable)

                consume(leftTopic)
                    .filterNotNull("skip-right-tombstone")
                    .join(leftTopic with rightTopic, ktable) { l, r -> "$l and $r" }
                    .produce(bothTopic, "produced-both")
            }.build()

            val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
            val kafka = TopologyTestDriver(topology, config)
            outputTopic(kafka, bothTopic)
            inputTopic(kafka, rightTopic)
            inputTopic(kafka, leftTopic)

            assertEquals("${leftTopic.name}-joined-${rightTopic.name}", topology.processorName("skip-right-tombstone"))
        }

        @Test
        fun `can leftJoin`() {
            val leftTopic = Topic("left", JsonSerde.jackson<String>())
            val rightTopic = Topic("right", JsonSerde.jackson<String>())
            val rightTable = Table("right", rightTopic)
            val bothTopic = Topic("both", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                val ktable = consume(rightTopic).produce(rightTable)

                consume(leftTopic)
                    .filterNotNull("skip-right-tombstone")
                    .leftJoin(leftTopic with rightTopic, ktable) { l, r -> "$l and $r" }
                    .produce(bothTopic, "produced-both")
            }.build()

            val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
            val kafka = TopologyTestDriver(topology, config)
            val outputTopic = outputTopic(kafka, bothTopic)
            inputTopic(kafka, leftTopic).pipeInput("123", "apple")

            val result = outputTopic.readKeyValue()
            assertEquals("123", result.key)
            assertEquals("apple and null", result.value)
        }

        @Test
        fun `leftJoin is Named`() {
            val leftTopic = Topic("left", JsonSerde.jackson<String>())
            val rightTopic = Topic("right", JsonSerde.jackson<String>())
            val rightTable = Table("right", rightTopic)
            val bothTopic = Topic("both", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                val ktable = consume(rightTopic).produce(rightTable)

                consume(leftTopic)
                    .filterNotNull("skip-right-tombstone")
                    .leftJoin(leftTopic with rightTopic, ktable) { l, r -> "$l and $r" }
                    .produce(bothTopic, "produced-both")
            }.build()

            val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
            val kafka = TopologyTestDriver(topology, config)
            outputTopic(kafka, bothTopic)
            inputTopic(kafka, rightTopic)
            inputTopic(kafka, leftTopic)

            assertEquals("${leftTopic.name}-joined-${rightTopic.name}", topology.processorName("skip-right-tombstone"))
        }
    }

    @Nested
    inner class SinkStream {
        @Test
        fun `can produce to topic`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .mapValues { _, value -> value!! }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            val kafka = TopologyTestDriver(topology)
            val sinkOutputTopic = outputTopic(kafka, sinkTopic)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")

            val result = sinkOutputTopic.readKeyValuesToList()
            assertEquals(1, result.size)
            assertEquals("123", result.first().key)
            assertEquals("hello", result.first().value)
        }

        @Test
        fun `can produce to table`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTable = Table("strings", sourceTopic)

            val topology = StreamsBuilder().apply {
                consume(sourceTopic).produce(sinkTable)
            }.build()

            val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
            val kafka = TopologyTestDriver(topology, config)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")

            val stateStore = kafka.getKeyValueStore<String, String>("strings-state-store")
            assertEquals("hello", stateStore["123"])
        }

        @Test
        fun `produced to table is logged`() {
            val log = SecureLogAppender()
                .apply { context = LoggerFactory.getILoggerFactory() as LoggerContext }
                .also {
                    (LoggerFactory.getLogger("secureLog") as Logger).apply {
                        level = Level.TRACE
                        addAppender(it)
                    }
                }
                .apply { start() }

            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTable = Table("strings", sourceTopic)

            val topology = StreamsBuilder().apply {
                consume(sourceTopic).produce(sinkTable, true)
            }.build()

            val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
            val kafka = TopologyTestDriver(topology, config)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")

            val secureMsg = log.firstContaining("Produserer")
            val args = secureMsg.structuredArguments()

            assertEquals("Produserer til KTable", secureMsg.message)
            assertEquals("123", args["key"])
            assertEquals("hello", args["value"])
            assertEquals("source", args["topic"])
            assertEquals("strings", args["table"])
            assertEquals("strings-state-store", args["store"])
            assertEquals("0", args["partition"])
            assertEquals("0", args["offset"])
        }

        @Test
        fun `produced to table is logged with Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTable = Table("strings", sourceTopic)

            val topology = StreamsBuilder().apply {
                consume(sourceTopic).produce(sinkTable)
            }.build()

            val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
            val kafka = TopologyTestDriver(topology, config)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")

            val expected = "log-consume-${sourceTopic.name}"
            val actual = topology.processorName("consume-${sourceTopic.name}")
            assertEquals(expected, actual)
        }

        @Test
        fun `produced to table is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTable = Table("strings", sourceTopic)

            val topology = StreamsBuilder().apply {
                consume(sourceTopic).produce(sinkTable)
            }.build()

            val config = Properties().apply { this[StreamsConfig.STATE_DIR_CONFIG] = "build/kafka-streams/state" }
            val kafka = TopologyTestDriver(topology, config)
            inputTopic(kafka, sourceTopic).pipeInput("123", "hello")

            assertEquals(
                expected = "${sinkTable.name}-as-table",
                actual = topology.processorName("log-produced-${sinkTable.name}")
            )
        }

        @Test
        fun `produced topic is Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .mapValues { _, value -> value!! }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            TopologyTestDriver(topology).apply {
                outputTopic(this, sinkTopic)
                inputTopic(this, sourceTopic).pipeInput("123", "hello")
            }

            assertEquals("produce-sink", topology.singleSinkName)
        }

        @Test
        fun `produced topic is logged`() {
            val log = SecureLogAppender()
                .apply { context = LoggerFactory.getILoggerFactory() as LoggerContext }
                .also {
                    (LoggerFactory.getLogger("secureLog") as Logger).apply {
                        level = Level.TRACE
                        addAppender(it)
                    }
                }
                .apply { start() }

            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .mapValues { _, value -> value!! }
                    .produce(sinkTopic, "produce-sink")
            }.build()

            TopologyTestDriver(topology).apply {
                outputTopic(this, sinkTopic)
                inputTopic(this, sourceTopic).pipeInput("123", "hello")
            }

            val secureMsg = log.firstContaining("Produserer")
            val args = secureMsg.structuredArguments()
            assertEquals("Produserer til Topic", secureMsg.message)
            assertEquals("123", args["key"])
            assertEquals("sink", args["topic"])
            assertEquals("0", args["partition"])
            assertEquals("0", args["offset"])
        }

        @Test
        fun `produced topic is logged with Named`() {
            val sourceTopic = Topic("source", JsonSerde.jackson<String>())
            val sinkTopic = Topic("sink", JsonSerde.jackson<String>())

            val topology = StreamsBuilder().apply {
                consume(sourceTopic)
                    .mapValues({ _, value -> value!! }, Named.`as`("processor-node"))
                    .produce(sinkTopic, "produce-sink")
            }.build()

            TopologyTestDriver(topology).apply {
                outputTopic(this, sinkTopic)
                inputTopic(this, sourceTopic).pipeInput("123", "hello")
            }

            assertEquals("log-produce-sink", topology.processorName("processor-node"))
        }
    }
}

private val Topology.nodes
    get() = describe().subtopologies().flatMap { sub -> sub.nodes() }

private val Topology.singleSourceName
    get() = nodes.filterIsInstance<TopologyDescription.Source>().map { it.name() }.single()

private val Topology.singleSinkName
    get() = nodes.filterIsInstance<TopologyDescription.Sink>().map { it.name() }.single()

private val Topology.singleProcessorName
    get() = nodes.filterIsInstance<TopologyDescription.Processor>().map { it.name() }.single()

private fun Topology.processorName(predeccessor: String) =
    nodes.filterIsInstance<TopologyDescription.Processor>()
        .single { processorNode -> processorNode.predecessors().map { it.name() }.single() == predeccessor }
        .name()

private fun outputTopic(kafka: TopologyTestDriver, sinkTopic: Topic<String>) =
    kafka.createOutputTopic(
        sinkTopic.name,
        sinkTopic.keySerde.deserializer(),
        sinkTopic.valueSerde.deserializer()
    )

private fun inputTopic(testDriver: TopologyTestDriver, sourceTopic: Topic<String>) =
    testDriver.createInputTopic(
        sourceTopic.name,
        sourceTopic.keySerde.serializer(),
        sourceTopic.valueSerde.serializer()
    )
