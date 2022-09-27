package no.nav.aap.kafka.streams

import no.nav.aap.kafka.SslConfig
import no.nav.aap.kafka.streams.handler.EntryPointExceptionHandler
import no.nav.aap.kafka.streams.handler.ExitPointExceptionHandler
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.streams.StreamsConfig.*
import java.util.*

data class KStreamsConfig(
    internal val applicationId: String,
    internal val brokers: String,
    internal val ssl: SslConfig? = null,
    internal val schemaRegistryProperties: Properties? = null,
    /**
     * Buffer across threads. For T threads with C cacehd bytes, each thread will get C/T cache bytes.
     * AAP vedtak uses 12 partitions, hence 120MB / 12 threads = 10 MB per partition.
     * disable (0) to increase performance.
     * Increase (e.g 120MB = 120 * 1024 * 1024) when aggregation or reduce is needed
     */
    internal val bytesToCacheBeforeCommitInterval: Long = 0,
    /**
     * If enabling cache, this should be increased
     * Defaults to EOS_DEFAULT_COMMIT_INTERVAL_MS (100ms) when set to null
     */
    internal val commitIntervalInMillis: Long? = null, // defaults to
) {
    fun streamsProperties(): Properties = Properties().apply {
        /* Replaces client-id and group-id, also used by NAIS to allow app to manage internal kafka topics on aiven */
        this[APPLICATION_ID_CONFIG] = applicationId

        /* A list of minimum 3 brokers (bootstrap servers) */
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers

        this[CACHE_MAX_BYTES_BUFFERING_CONFIG] = bytesToCacheBeforeCommitInterval

        if (commitIntervalInMillis != null) {
            this[COMMIT_INTERVAL_MS_CONFIG] = commitIntervalInMillis
        }

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
