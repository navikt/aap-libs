package libs.kafka.client

import libs.kafka.StreamsConfig
import libs.kafka.Topic
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*

interface ProducerFactory {

    fun <V : Any> createProducer(
        streamsConfig: StreamsConfig,
        topic: Topic<V>,
    ): Producer<String, V> {
        val producerConfig = ProducerFactoryConfig(
            streamsConfig = streamsConfig,
            clientId = "${streamsConfig.applicationId}-producer-${topic.name}",
        )
        return KafkaProducer(
            producerConfig.toProperties(),
            topic.keySerde.serializer(),
            topic.valueSerde.serializer(),
        )
    }
}

private class ProducerFactoryConfig(
    private val streamsConfig: StreamsConfig,
    private val clientId: String,
) {
    fun toProperties(): Properties = Properties().apply {
        this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId
        this[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = streamsConfig.brokers
        this[ProducerConfig.ACKS_CONFIG] = "all"
        this[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = "5"
        this[ProducerConfig.COMPRESSION_TYPE_CONFIG] = streamsConfig.compressionType

        streamsConfig.ssl?.let { putAll(it.properties()) }
        streamsConfig.schemaRegistry?.let { putAll(it.properties()) }
    }
}
