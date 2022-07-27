package no.nav.aap.kafka.vanilla

import no.nav.aap.kafka.SslConfig
import no.nav.aap.kafka.streams.KStreamsConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

data class KafkaConfig(
    private val brokers: String,
    private val ssl: SslConfig?,
    private val schemaRegistryProperties: Properties?,
) {
    companion object {
        fun copyFrom(streamsConfig: KStreamsConfig): KafkaConfig = KafkaConfig(
            brokers = streamsConfig.brokers,
            ssl = streamsConfig.ssl,
            schemaRegistryProperties = streamsConfig.schemaRegistryProperties
        )
    }

    fun consumerProperties(clientId: String, groupId: String): Properties = Properties().apply {
        this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokers

        /* Can be freely set to earliest or latest. Is associated with the group-id */
        this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        /* Unique client-id (kafka streams application-id is occupied) */
        this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId

        /* Unique group-id (kafka streams application-id is occupied) */
        this[ConsumerConfig.GROUP_ID_CONFIG] = groupId

        /* Set to 2min + estimated max processing time (e.g. 4 sec) */
        this[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 124_000

        /* Security config for accessing Aiven */
        ssl?.let { putAll(it.properties()) }

        /* Optional use of schema registry - required for avro and protobuf */
        schemaRegistryProperties?.let { putAll(it) }
    }

    fun producerProperties(clientId: String): Properties = Properties().apply {
        this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers
        this[ProducerConfig.ACKS_CONFIG] = "all"
        this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = "5"

        /* Security config for accessing Aiven */
        ssl?.let { putAll(it.properties()) }

        /* Optional use of schema registry - required for avro and protobuf */
        schemaRegistryProperties?.let { putAll(it) }
    }
}
