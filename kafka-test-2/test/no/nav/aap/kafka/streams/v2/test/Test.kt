package no.nav.aap.kafka.streams.v2.test

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.streams.v2.Table
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.config.KStreamsConfig
import no.nav.aap.kafka.streams.v2.config.SslConfig
import no.nav.aap.kafka.streams.v2.topology
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class Test {

    @Test
    fun `join topic with table`() {
        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)
            consume(Topics.A)
                .joinWith(table)
                .map { a, b -> b + a }
                .produce(Topics.C)
        }

        val kafka = KStreamsMock()
        val registry = SimpleMeterRegistry()
        val config = KStreamsConfig(
            applicationId = "app",
            brokers = "mock://kafka",
            ssl = SslConfig("", "", ""),
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
    val A = Topic("A", Serdes.StringSerde())
    val B = Topic("B", Serdes.StringSerde())
    val C = Topic("C", Serdes.StringSerde())
}

internal object Tables {
    val B = Table(Topics.B)
}