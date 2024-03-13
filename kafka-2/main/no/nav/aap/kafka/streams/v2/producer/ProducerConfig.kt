package no.nav.aap.kafka.streams.v2.producer

import no.nav.aap.kafka.streams.v2.config.SslConfig
import no.nav.aap.kafka.streams.v2.config.StreamsConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

class ProducerConfig private constructor(
    private val brokers: String,
    private val ssl: SslConfig?,
    private val schemaRegistry: Properties?,
    private val compressionType: String,
) {

    constructor(streamsConfig: StreamsConfig) : this(
        brokers = streamsConfig.brokers,
        ssl = streamsConfig.ssl,
        schemaRegistry = streamsConfig.schemaRegistry.properties(),
        compressionType = streamsConfig.compressionType
    )

    fun toProperties(clientId: String): Properties = Properties().apply {
        this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers
        this[ProducerConfig.ACKS_CONFIG] = "all"
        this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = "5"
        this[ProducerConfig.COMPRESSION_TYPE_CONFIG] = compressionType

        /* Security config for accessing Aiven */
        ssl?.let { putAll(it.properties()) }

        /* Optional use of schema registry - required for avro and protobuf */
        schemaRegistry?.let { putAll(it) }
    }
}