package no.nav.aap.kafka.streams.v2

import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class ConsumeTest {
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

    @Test
    fun `consume and use custom processor`() {
        val topology = topology {
            consume(Topics.A)
                .processor(::CustomProcessor)
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.A).produce("1", "a")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("a.v2", result["1"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }

    @Test
    fun `consume and use custom processor with table`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .processor(table) { CustomProcessorWithTable(Tables.B) }
                .produce(Topics.C)
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", ".v2")
        kafka.inputTopic(Topics.A).produce("1", "a")

        val result = kafka.outputTopic(Topics.C).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("a.v2", result["1"])

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology))
    }
}

class CustomProcessorWithTable<T>(private val table: Table<T>) : CustomProcessor() {
    private lateinit var store: TimestampedKeyValueStore<String, String>

    override fun init(context: FixedKeyProcessorContext<String, String>) {
        super.init(context)
        this.store = context.getStateStore(table.stateStoreName)
    }

    override fun process(record: FixedKeyRecord<String, String>) {
        val storedValue = store[record.key()].value()
        context.forward(record.withValue("${record.value()}$storedValue"))
    }
}

open class CustomProcessor : FixedKeyProcessor<String, String, String> {
    protected lateinit var context: FixedKeyProcessorContext<String, String>

    override fun init(context: FixedKeyProcessorContext<String, String>) {
        this.context = context
    }

    override fun process(record: FixedKeyRecord<String, String>) {
        context.forward(record.withValue("${record.value()}.v2"))
    }

    override fun close() {}
}
