package no.nav.aap.kafka.vanilla

import no.nav.aap.kafka.SslConfig
import no.nav.aap.kafka.schemaregistry.SchemaRegistryConfig
import no.nav.aap.kafka.streams.KStreamsConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

data class KafkaConfig(
    private val brokers: String,
    private val ssl: SslConfig?,
    private val schemaRegistry: SchemaRegistryConfig?,
) {
    companion object {
        fun copyFrom(streamsConfig: KStreamsConfig): KafkaConfig = KafkaConfig(
            brokers = streamsConfig.brokers,
            ssl = streamsConfig.ssl,
            schemaRegistry = streamsConfig.schemaRegistry
        )
    }

    fun consumerProperties(clientId: String, groupId: String): Properties = Properties().apply {
        putAll(defaults())
        this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId
        this[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokers
        this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        // Should be 2 min + max processing time (e.g 4 sec)
        this[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = 124_000
    }

    fun producerProperties(clientId: String): Properties = Properties().apply {
        putAll(defaults())
        this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = brokers
        this[ProducerConfig.ACKS_CONFIG] = "all"
        this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = "5"
    }

    private fun defaults(): Properties = Properties().apply {
        ssl?.properties()?.let { putAll(it) }
        schemaRegistry?.properties()?.let { putAll(it) }
    }
}
