package no.nav.aap.kafka.vanilla

import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer

interface KafkaFactory {
    fun <V : Any> createConsumer(config: KafkaConfig, topic: Topic<V>): Consumer<String, V> = KafkaConsumer(
        config.consumerProperties("consumer-${topic.name}", "${topic.name}-1"),
        topic.keySerde.deserializer(),
        topic.valueSerde.deserializer(),
    )

    fun <V : Any> createProducer(config: KafkaConfig, topic: Topic<V>): Producer<String, V> = KafkaProducer(
        config.producerProperties("producer-${topic.name}"),
        topic.keySerde.serializer(),
        topic.valueSerde.serializer()
    )
}
