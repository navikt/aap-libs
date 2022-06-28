package no.nav.aap.kafka

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import no.nav.aap.kafka.schemaregistry.SchemaRegistryConfig
import no.nav.aap.kafka.streams.KStreamsConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class ConfigTest {

    @Test
    fun `schemaRegistry is empty without schema registry url`() {
        val config = KStreamsConfig(
            applicationId = "app",
            brokers = "localhost:9092",
        )

        assertNull(config.schemaRegistry)
    }

    @Test
    fun `schema registry config is configured when present`() {
        val config = KStreamsConfig(
            applicationId = "app",
            brokers = "localhost:9092",
            schemaRegistry = schemaConfig,
        )

        val expectedConfig = Properties().apply {
            this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "localhost:8081"
            this[SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE] = "USER_INFO"
            this[SchemaRegistryClientConfig.USER_INFO_CONFIG] = "user:password"
        }

        assertEquals(expectedConfig, config.schemaRegistry?.properties())
    }

    @Test
    fun `required streams config is present`() {
        val config = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092")
        val streamsProps = config.streamsProperties()
        assertEquals("test-app", streamsProps.getProperty("application.id", "missing"))
        assertEquals("localhost:9092", streamsProps.getProperty("bootstrap.servers", "missing"))
    }

    @Test
    fun `kafka streams has exactly onces turned on`() {
        val config = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092")
        val streamsProps = config.streamsProperties()
        assertEquals("exactly_once_v2", streamsProps.getProperty("processing.guarantee", "missing"))
    }

    @Test
    fun `ssl is configured with available certificates`() {
        val config = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092", ssl = sslConfig)

        val expectedConfig = Properties().apply {
            this[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SSL"
            this[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = "JKS"
            this[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = "trust"
            this[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = "cred"
            this[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = "PKCS12"
            this[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = "key"
            this[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = "cred"
            this[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = "cred"
            this[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = ""
        }

        assertEquals(expectedConfig, config.ssl?.properties())
    }
}

val sslConfig = SslConfig(
    truststorePath = "trust",
    keystorePath = "key",
    credstorePsw = "cred"
)

val schemaConfig = SchemaRegistryConfig(
    url = "localhost:8081",
    user = "user",
    password = "password",
)
