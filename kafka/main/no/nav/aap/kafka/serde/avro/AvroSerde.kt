package no.nav.aap.kafka.serde.avro

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import no.nav.aap.kafka.plus
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import java.util.*

object AvroSerde {
    fun <T : SpecificRecord> specific(): SpecificAvroSerde<T> = SpecificAvroSerde<T>().apply {
        val schemaRegistry = Properties().apply {
            val url = System.getenv("KAFKA_SCHEMA_REGISTRY")
            val user = System.getenv("KAFKA_SCHEMA_REGISTRY_USER")
            val pwd = System.getenv("KAFKA_SCHEMA_REGISTRY_PASSWORD")
            this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = url
            this[SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE] = "USER_INFO"
            this[SchemaRegistryClientConfig.USER_INFO_CONFIG] = "$user:$pwd"
        }

        val ssl: Properties = Properties().apply {
            val keystorePath = System.getenv("KAFKA_KEYSTORE_PATH")
            val truststorePath = System.getenv("KAFKA_TRUSTSTORE_PATH")
            val credstorePsw = System.getenv("KAFKA_CREDSTORE_PASSWORD")
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

        val avroProperties = schemaRegistry + ssl
        val avroConfig = avroProperties.map { it.key.toString() to it.value.toString() }
        configure(avroConfig.toMap(), false)
    }

    fun generic() = GenericAvroSerde()
}
