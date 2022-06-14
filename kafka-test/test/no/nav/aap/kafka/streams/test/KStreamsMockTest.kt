package no.nav.aap.kafka.streams.test

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.KafkaConfig
import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.StreamsBuilder
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

private fun KafkaStreamsMock.schemaRegistryUrl(): String? =
    javaClass.getDeclaredField("schemaRegistryUrl").let {
        it.isAccessible = true
        return@let it.get(this) as? String;
    }

class KStreamsMockTest {

    @Test
    fun `streams without schema registry`() {
        val kafka = KafkaStreamsMock()
        val registry = SimpleMeterRegistry()
        val config = KafkaConfig(
            "app",
            "mock://kafka",
            "c",
            "",
            "",
            "",
            null,
            null,
            null,
        )

        kafka.connect(config, registry, StreamsBuilder().build())

        assertNull(kafka.schemaRegistryUrl())
    }

    @Test
    fun `streams with schema registry`() {
        val kafka = KafkaStreamsMock()
        val registry = SimpleMeterRegistry()
        val schemaUrl = "mock://schema-reg"
        val config = KafkaConfig(
            "app",
            "mock://kafka",
            "c",
            "",
            "",
            "",
            schemaUrl,
            null,
            null,
        )

        kafka.connect(config, registry, StreamsBuilder().build())

        assertNotNull(UUID.fromString(kafka.schemaRegistryUrl()?.removePrefix("$schemaUrl/")))
    }

    @Test
    fun `Ny producer`() {
        val kafka = KafkaStreamsMock()
        val config = KafkaConfig(
            "app",
            "mock://kafka",
            "c",
            "",
            "",
            "",
            null,
            null,
            null,
        )
        val topic = Topic<String>("test", JsonSerde.jackson())
        kafka.createProducer(config, topic).send(ProducerRecord("test", "123", "val"))
        val producer = kafka.getProducer(topic)
        assertEquals(1, producer.history().size)
        assertEquals("val", producer.history().single().value())
    }
}
