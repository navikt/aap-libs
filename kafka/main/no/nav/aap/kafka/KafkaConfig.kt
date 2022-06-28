package no.nav.aap.kafka

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import no.nav.aap.kafka.streams.EntryPointExceptionHandler
import no.nav.aap.kafka.streams.ExitPointExceptionHandler
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp
import java.util.*

operator fun Properties.plus(properties: Properties): Properties = apply { putAll(properties) }
operator fun Properties.plus(properties: Map<String, String>): Properties = apply { putAll(properties) }

data class KafkaConfig(
    private val applicationId: String,
    private val brokers: String,
    private val clientId: String,
    private val truststorePath: String,
    private val keystorePath: String,
    private val credstorePsw: String,
    private val schemaRegistryUrl: String?,
    private val schemaRegistryUser: String?,
    private val schemaRegistryPwd: String?,
) {
    fun schemaProperties() =
        Properties().apply {
            if (schemaRegistryUrl != null) {
                this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
                if (schemaRegistryUser != null && schemaRegistryPwd != null) {
                    this[SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE] = "USER_INFO"
                    this[SchemaRegistryClientConfig.USER_INFO_CONFIG] = "$schemaRegistryUser:$schemaRegistryPwd"
                }
            }
        }

    fun streamsProperties() =
        Properties().apply {
            this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId

            this[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId

            // The max amount of bytes buffered used across all threads. If not doing aggregation, this can be set to 0, which is also default.
            this[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = "0"

//            this[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = ByteArraySerde::class.java.name
//            this[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = StringSerde::class.java.name

            this[StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG] = ExitPointExceptionHandler::class.java.name
            this[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = LogAndSkipOnInvalidTimestamp::class.java.name
            this[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] = EntryPointExceptionHandler::class.java.name

            // Committed when: (1) acked produce to sink (2) state update of app (3) offset commit on source
            this[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2
        }

    fun sslProperties() =
        Properties().apply {
            if (keystorePath.isNotEmpty() && truststorePath.isNotEmpty() && credstorePsw.isNotEmpty()) {
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

    fun consumerProperties(clientId: String, groupId: String) =
        schemaProperties() + sslProperties() + Properties().apply {
            this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId
            this[ConsumerConfig.GROUP_ID_CONFIG] = groupId
            this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokers
            this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            // 2 min + worst case processing time (4 sec) before timeout
            this[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 124_000
        }

    fun producerProperties(clientId: String) =
        schemaProperties() + sslProperties() + Properties().apply {
            this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId
            this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers
            this[ProducerConfig.ACKS_CONFIG] = "all"
            this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = "5"
        }
}
