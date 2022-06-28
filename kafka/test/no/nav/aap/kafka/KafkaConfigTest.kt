package no.nav.aap.kafka

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals

internal class KafkaConfigTest {

    @Test
    fun `schemaRegistry is empty without schema registry url`() {
        val config = defaultKafkaTestConfig.copy(schemaRegistryUrl = null)
        assertEquals(Properties(), config.schemaProperties())
    }

    @Test
    fun `schemaRegistry is configured with schema registry url`() {
        val config = defaultKafkaTestConfig.copy(keystorePath = "")
        val expectedConfig = Properties().apply {
            this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "localhost:8081"
        }
        assertEquals(expectedConfig, config.schemaProperties())
    }

    @Test
    fun `schemaRegistry with schema credentials is configured with basic auth`() {
        val config = defaultKafkaTestConfig.copy(schemaRegistryUser = "usr", schemaRegistryPwd = "pwd")
        val expectedConfig = Properties().apply {
            this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = "localhost:8081"
            this[SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE] = "USER_INFO"
            this[SchemaRegistryClientConfig.USER_INFO_CONFIG] = "usr:pwd"
        }
        assertEquals(expectedConfig, config.schemaProperties())
    }

    @Test
    fun `ssl is empty without keystorePath`() {
        val config = defaultKafkaTestConfig.copy(keystorePath = "")
        assertEquals(Properties(), config.sslProperties())
    }

    @Test
    fun `ssl is empty without truststorePath`() {
        val config = defaultKafkaTestConfig.copy(truststorePath = "")
        assertEquals(Properties(), config.sslProperties())
    }

    @Test
    fun `ssl is empty without credstorePsw`() {
        val config = defaultKafkaTestConfig.copy(credstorePsw = "")
        assertEquals(Properties(), config.sslProperties())
    }

    @Test
    fun `ssl is configured with available certificates`() {
        val config = defaultKafkaTestConfig
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
        assertEquals(expectedConfig, config.sslProperties())
    }
}

val defaultKafkaTestConfig = KafkaConfig(
    "app",
    "localhost:9092",
    "client",
    "trust",
    "key",
    "cred",
    "localhost:8081",
    null,
    null,
)
