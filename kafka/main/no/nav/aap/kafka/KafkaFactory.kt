package no.nav.aap.kafka

import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer

interface KafkaFacade {
    fun <V : Any> createConsumer(topic: Topic<V>): Consumer<String, V>
    fun <V : Any> createProducer(topic: Topic<V>): Producer<String, V>
}

class KafkaFactory(private val config: KafkaConfig) : KafkaFacade {
    override fun <V : Any> createConsumer(topic: Topic<V>): Consumer<String, V> =
        KafkaConsumer(
            config.consumer + mapOf(CommonClientConfigs.CLIENT_ID_CONFIG to "client-${topic.name}"),
            topic.keySerde.deserializer(),
            topic.valueSerde.deserializer()
        )

    override fun <V : Any> createProducer(topic: Topic<V>): Producer<String, V> =
        KafkaProducer(
            config.producer + mapOf(CommonClientConfigs.CLIENT_ID_CONFIG to "client-${topic.name}"),
            topic.keySerde.serializer(),
            topic.valueSerde.serializer()
        )
}
