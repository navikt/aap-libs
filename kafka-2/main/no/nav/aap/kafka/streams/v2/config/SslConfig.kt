package no.nav.aap.kafka.streams.v2.config

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import java.util.*

data class SslConfig(
    private val truststorePath: String,
    private val keystorePath: String,
    private val credstorePsw: String,
) {
    fun properties() = Properties().apply {
        this[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SSL"
        this[SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG] = "JKS"
        this[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = truststorePath
        this[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = credstorePsw
        this[SslConfigs.SSL_KEYSTORE_TYPE_CONFIG] = "PKCS12"
        this[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = keystorePath
        this[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = credstorePsw
        this[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = credstorePsw
        this[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = ""
    }

    companion object {
        val DEFAULT: SslConfig by lazy {
            SslConfig(
                truststorePath = System.getenv("KAFKA_TRUSTSTORE_PATH"),
                keystorePath = System.getenv("KAFKA_KEYSTORE_PATH"),
                credstorePsw = System.getenv("KAFKA_CREDSTORE_PASSWORD")
            )
        }
    }
}
