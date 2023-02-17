package no.nav.aap.kafka.streams.v2.producer

import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.config.StreamsConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer

interface ProducerFactory {

    fun <V : Any> createProducer(streamsConfig: StreamsConfig, topic: Topic<V>): Producer<String, V> {
        val properties = ProducerConfig(streamsConfig).toProperties(
            clientId = "${streamsConfig.applicationId}-producer-${topic.name}"
        )
        return KafkaProducer(properties, topic.keySerde.serializer(), topic.valueSerde.serializer())
    }
}
