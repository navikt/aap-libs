package no.nav.aap.kafka.streams.v2.config

import java.util.*

private fun getEnvVar(envar: String) = System.getenv(envar) ?: error("missing envvar $envar")

data class SchemaRegistryConfig(
    private val url: String = getEnvVar("KAFKA_SCHEMA_REGISTRY"),
    private val user: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_USER"),
    private val password: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_PASSWORD"),
) {
    fun properties() = Properties().apply {
        this["schema.registry.url"] = url
        this["basic.auth.credentials.source"] = "USER_INFO"
        this["basic.auth.user.info"] = "$user:$password"
    }
}
