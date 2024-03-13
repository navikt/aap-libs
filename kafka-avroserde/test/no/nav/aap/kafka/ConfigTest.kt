package no.nav.aap.kafka

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import no.nav.aap.kafka.streams.v2.config.SchemaRegistryConfig
import no.nav.aap.kafka.streams.v2.config.SslConfig
import no.nav.aap.kafka.streams.v2.config.StreamsConfig
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class ConfigTest {

    @Test
    fun `schema registry config is configured when present`() {
        val config = StreamsConfig(
            applicationId = "app",
            brokers = "localhost:9092",
            schemaRegistry = schemaConfig,
            ssl = SslConfig("", "", "")
        )

        config.streamsProperties().apply {
            assertEquals("localhost:8081", this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG])
            assertEquals("USER_INFO", this[SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE])
            assertEquals("user:password", this[SchemaRegistryClientConfig.USER_INFO_CONFIG])
        }
    }
}

val schemaConfig = SchemaRegistryConfig(
    url = "localhost:8081",
    user = "user",
    password = "password",
)
