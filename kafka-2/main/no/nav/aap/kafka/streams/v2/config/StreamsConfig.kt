package no.nav.aap.kafka.streams.v2.config

import no.nav.aap.kafka.streams.v2.exception.EntryPointExceptionHandler
import no.nav.aap.kafka.streams.v2.exception.ExitPointExceptionHandler
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import java.util.*

private fun getEnvVar(envar: String) = System.getenv(envar) ?: error("missing envvar $envar")

data class StreamsConfig(
    val applicationId: String = getEnvVar("KAFKA_STREAMS_APPLICATION_ID"),
    val brokers: String = getEnvVar("KAFKA_BROKERS"),
    val ssl: SslConfig? = SslConfig(),
    val schemaRegistry: SchemaRegistryConfig = SchemaRegistryConfig(),
    val compressionType: String = "snappy",
    val additionalProperties: Properties = Properties(),
) {
    fun streamsProperties(): Properties = Properties().apply {
        this[StreamsConfig.APPLICATION_ID_CONFIG] = applicationId
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers

        ssl?.let { putAll(it.properties()) }
        putAll(schemaRegistry.properties())
        putAll(additionalProperties)

        /* Exception handler when leaving the stream, e.g. serialization */
        this[StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG] =
            ExitPointExceptionHandler::class.java.name

        /*  Exception handler when entering the stream, e.g. deserialization */
        this[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] =
            EntryPointExceptionHandler::class.java.name

        // Configuration for resilience
        this[StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG)] = "all"
        this[StreamsConfig.REPLICATION_FACTOR_CONFIG] = 3
        this[StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG] = 1

        // Configuration for decreaseing latency
        this[StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG)] = 0 // do not batch
        this[StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG)] = 0 // send immediately

        // Configuration for message size
        this[StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG)] = compressionType

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
        this[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2
    }
}
