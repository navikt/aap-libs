package no.nav.aap.kafka.streams

import no.nav.aap.kafka.SslConfig
import no.nav.aap.kafka.streams.handler.EntryPointExceptionHandler
import no.nav.aap.kafka.streams.handler.ExitPointExceptionHandler
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.StreamsConfig.*
import java.util.*

data class KStreamsConfig(
    internal val applicationId: String,
    internal val brokers: String,
    internal val ssl: SslConfig? = null,
    internal val schemaRegistryProperties: Properties? = null,
) {
    fun streamsProperties(): Properties = Properties().apply {
        /* Replaces client-id and group-id, also used by NAIS to allow app to manage internal kafka topics on aiven */
        this[APPLICATION_ID_CONFIG] = applicationId

        /* A list of minimum 3 brokers (bootstrap servers) */
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers

        /* Buffer across threads, set to 0 for performance or increase when doing aggregate/reduce (e.g. 10MB) */
        this[CACHE_MAX_BYTES_BUFFERING_CONFIG] = "1048576"
        this[COMMIT_INTERVAL_MS_CONFIG] = 10_000

        /* Exception handler when leaving the stream, e.g. serialization */
        this[DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG] = ExitPointExceptionHandler::class.java.name

        /*  Exception handler when entering the stream, e.g. deserialization */
        this[DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] = EntryPointExceptionHandler::class.java.name

        /* Security config for accessing Aiven */
        ssl?.let { putAll(it.properties()) }

        /* Required for avro and protobuf */
        schemaRegistryProperties?.let { putAll(it) }

        /**
         * Enable exactly onces semantics:
         * 1. ack produce to sink topic
         * 2. update state in app (state store)
         * 3. commit offset for source topic
         *
         * commit.interval.ms is set to 100ms
         * comsumers are configured with isolation.level="read_committed"
         * processing requires minimum three brokers
         */
        this[PROCESSING_GUARANTEE_CONFIG] = EXACTLY_ONCE_V2
    }
}
