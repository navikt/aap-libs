package no.nav.aap.kafka

import no.nav.aap.kafka.streams.KStreamsConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.test.assertEquals

internal class ConfigTest {

    @Test
    fun `required streams config is present`() {
        val config = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092", ssl = SslConfig("", "", ""))
        val streamsProps = config.streamsProperties()
        assertEquals("test-app", streamsProps.getProperty("application.id", "missing"))
        assertEquals("localhost:9092", streamsProps.getProperty("bootstrap.servers", "missing"))
    }

    @Test
    fun `kafka streams has exactly onces turned on`() {
        val config = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092", ssl = SslConfig("", "", ""))
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
