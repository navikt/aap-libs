package no.nav.aap.kafka.streams

import no.nav.aap.kafka.SslConfig
import no.nav.aap.kafka.streams.handler.EntryPointExceptionHandler
import no.nav.aap.kafka.streams.handler.ExitPointExceptionHandler
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG
import org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2
import org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG
import java.util.*

data class KStreamsConfig(
    internal val applicationId: String,
    internal val brokers: String,
    internal val ssl: SslConfig? = null,
    internal val schemaRegistryProperties: Properties = Properties(),

    /** Cache C bytes for T threads (partitions) = C/T bytes per thread. Default (null) is 10MB */
    internal val cacheBytes: Long? = null,

    /** Commit interval to brokers. Default (null) is 100 ms */
    internal val commitIntervalMs: Long? = null,

    /** Override or add additional properties */
    internal val additionalProperties: Properties = Properties(),
) {
    fun streamsProperties(): Properties = Properties().apply {
        this[APPLICATION_ID_CONFIG] = applicationId
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers

        cacheBytes?.let { this[CACHE_MAX_BYTES_BUFFERING_CONFIG] = it }
        commitIntervalMs?.let { this[COMMIT_INTERVAL_MS_CONFIG] = it }
        ssl?.let { putAll(it.properties()) }

        putAll(schemaRegistryProperties)

        /* Exception handler when leaving the stream, e.g. serialization */
        this[DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG] = ExitPointExceptionHandler::class.java.name

        /*  Exception handler when entering the stream, e.g. deserialization */
        this[DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] = EntryPointExceptionHandler::class.java.name

        // Configuration for resilience
        this[StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG)] = "all"
        this[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 3
        this[StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG] = 1

        // Configuration for decreaseing latency
        this[StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG)] = 0 // do not batch
        this[StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG)] = 0 // send immediately
        this[StreamsConfig.MAX_TASK_IDLE_MS_CONFIG] = 3_000
//        this[StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG)] = "snappy"

        /*
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

    }.apply { putAll(additionalProperties) }
}
