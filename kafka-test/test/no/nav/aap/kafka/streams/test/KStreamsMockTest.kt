package no.nav.aap.kafka.streams.test

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.SslConfig
import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.KStreamsConfig
import no.nav.aap.kafka.streams.Topic
import no.nav.aap.kafka.vanilla.KafkaConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.StreamsBuilder
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class KStreamsMockTest {

    @Test
    fun `streams without schema registry`() {
        val kafka = KafkaStreamsMock()
        val registry = SimpleMeterRegistry()
        val config = KStreamsConfig(
            applicationId = "app",
            brokers = "mock://kafka",
            ssl = SslConfig("", "", ""),
        )

        kafka.connect(config, registry, StreamsBuilder().build())

        assertNull(kafka.schemaRegistryUrl)
    }

    @Test
    fun `streams with schema registry`() {
        val kafka = KafkaStreamsMock()
        val registry = SimpleMeterRegistry()
        val schemaUrl = "mock://schema-reg"
        val config = KStreamsConfig(
            applicationId = "app",
            brokers = "mock://kafka",
            schemaRegistryProperties = Properties().apply {
                this["schema.registry.url"] = schemaUrl
                this["basic.auth.credentials.source"] = "USER_INFO"
                this["basic.auth.user.info"] = "usr:pwd"
            },
            ssl = SslConfig("", "", ""),
        )

        kafka.connect(config, registry, StreamsBuilder().build())

        assertNotNull(UUID.fromString(kafka.schemaRegistryUrl?.removePrefix("$schemaUrl/")))
    }

    @Test
    fun `Ny producer`() {
        val kafka = KafkaStreamsMock()
        val config = KStreamsConfig(
            applicationId = "app",
            brokers = "mock://kafka",
            ssl = SslConfig("", "", "")
        )

        val topic = Topic<String>("test", JsonSerde.jackson())
        kafka.createProducer(KafkaConfig.copyFrom(config), topic).send(ProducerRecord("test", "123", "val"))
        val producer = kafka.getProducer(topic)
        assertEquals(1, producer.history().size)
        assertEquals("val", producer.history().single().value())
    }
}
