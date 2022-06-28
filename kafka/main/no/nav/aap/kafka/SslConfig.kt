package no.nav.aap.kafka

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
}
