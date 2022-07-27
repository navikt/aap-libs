package no.nav.aap.kafka.schemaregistry

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import java.util.*

data class SchemaRegistryConfig(
    private val url: String,
    private val user: String,
    private val password: String,
) {
    fun properties() = Properties().apply {
        this[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = url
        this[SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE] = "USER_INFO"
        this[SchemaRegistryClientConfig.USER_INFO_CONFIG] = "$user:$password"
    }
}
