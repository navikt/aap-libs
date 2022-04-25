package no.nav.aap.kafka

import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer

object KafkaFactoryMock : KafkaFacade {
    override fun <V : Any> createConsumer(config: KafkaConfig, topic: Topic<V>): Consumer<String, V> {
        return MockConsumer(OffsetResetStrategy.EARLIEST)
    }

    override fun <V : Any> createProducer(config: KafkaConfig, topic: Topic<V>): Producer<String, V> {
        return MockProducer()
    }
}