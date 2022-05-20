package no.nav.aap.kafka.streams.test

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.KafkaConfig
import org.junit.Test
import java.util.*
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

        kafka.start(config, registry) {}

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

        kafka.start(config, registry) {}

        assertNotNull(UUID.fromString(kafka.schemaRegistryUrl()?.removePrefix("$schemaUrl/")))
    }
}