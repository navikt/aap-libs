package no.nav.aap.kafka.streams

import no.nav.aap.kafka.SslConfig
import no.nav.aap.kafka.schemaregistry.SchemaRegistryConfig
import no.nav.aap.kafka.streams.handler.EntryPointExceptionHandler
import no.nav.aap.kafka.streams.handler.ExitPointExceptionHandler
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.streams.StreamsConfig.*
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp
import java.util.*

data class KStreamsConfig(
    internal val applicationId: String,
    internal val brokers: String,
    internal val ssl: SslConfig? = null,
    internal val schemaRegistry: SchemaRegistryConfig? = null,
) {
    fun streamsProperties(): Properties = Properties().apply {
        putAll(defaults())
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers
        this[APPLICATION_ID_CONFIG] = applicationId
        this[CACHE_MAX_BYTES_BUFFERING_CONFIG] = "0" // change to e.g. 10 MB if doing aggregation
        this[DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG] = ExitPointExceptionHandler::class.java.name
        this[DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = LogAndSkipOnInvalidTimestamp::class.java.name
        this[DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] = EntryPointExceptionHandler::class.java.name
        this[PROCESSING_GUARANTEE_CONFIG] = EXACTLY_ONCE_V2
    }

    private fun defaults(): Properties = Properties().apply {
        ssl?.properties()?.let { putAll(it) }
        schemaRegistry?.properties()?.let { putAll(it) }
    }
}
