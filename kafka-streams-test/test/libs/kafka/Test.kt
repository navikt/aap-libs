package libs.kafka

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import lib.kafka.StreamsMock
import libs.kafka.serde.StringSerde
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class Test {

    @Test
    fun `join topic with table`() {
        val topology = topology {
            val table = consume(Tables.B)
            consume(Topics.A)
                .joinWith(table)
                .map { a, b -> b + a }
                .produce(Topics.C)
        }

        val kafka = StreamsMock()
        val registry = SimpleMeterRegistry()
        val config = StreamsConfig(
            applicationId = "app",
            brokers = "mock://kafka",
            ssl = SslConfig("", "", ""),
            schemaRegistry = SchemaRegistryConfig("", "", ""),
        )

        kafka.connect(topology, config, registry)

        val a = kafka.testTopic(Topics.A)
        val b = kafka.testTopic(Topics.B)
        val c = kafka.testTopic(Topics.C)

        val key = "1"
        b.produce(key) { "B" }
        a.produce(key) { "A" }.produce("2") { "A" }

        val result = c.readValue()
        assertEquals("BA", result)

        println(kafka.visulize().uml())
    }

}

internal object Topics {
    val A = Topic("A", StringSerde)
    val B = Topic("B", StringSerde)
    val C = Topic("C", StringSerde)
}

internal object Tables {
    val B = Table(Topics.B)
}