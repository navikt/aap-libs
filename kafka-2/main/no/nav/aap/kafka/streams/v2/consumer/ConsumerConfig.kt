package no.nav.aap.kafka.streams.v2.consumer

import no.nav.aap.kafka.streams.v2.config.SslConfig
import no.nav.aap.kafka.streams.v2.config.StreamsConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.*

class ConsumerConfig private constructor(
    private val brokers: String,
    private val ssl: SslConfig?,
    private val schemaRegistry: Properties?,
) {

    enum class OffsetResetPolicy {
        earliest,
        latest
    }

    constructor(streamsConfig: StreamsConfig) : this(
        brokers = streamsConfig.brokers,

        ssl = streamsConfig.ssl,
        schemaRegistry = streamsConfig.schemaRegistry,
    )

    internal fun toProperties(
        clientId: String,
        groupId: String,
        offsetResetPolicy: OffsetResetPolicy,
    ): Properties = Properties().apply {
        this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokers

        /* Can be freely set to earliest or latest. Is associated with the group-id */
        this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = offsetResetPolicy.name

        /* Unique client-id (kafka streams application-id is occupied) */
        this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId

        /* Unique group-id (kafka streams application-id is occupied) */
        this[ConsumerConfig.GROUP_ID_CONFIG] = groupId

        /* Set to 2min + estimated max processing time (e.g. 4 sec) */
        this[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 124_000

        /* Security config for accessing Aiven */
        ssl?.let { putAll(it.properties()) }

        /* Optional use of schema registry - required for avro and protobuf */
        schemaRegistry?.let { putAll(it) }
    }

}