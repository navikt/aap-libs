package no.nav.aap.kafka.streams.v2.consumer

import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.config.StreamsConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer

interface ConsumerFactory {
    fun <V : Any> createConsumer(
        streamsConfig: StreamsConfig,
        topic: Topic<V>,
        groupIdSuffix: Int = 1,
        offsetResetPolicy: ConsumerConfig.OffsetResetPolicy = ConsumerConfig.OffsetResetPolicy.earliest
    ): Consumer<String, V> {
        val properties = ConsumerConfig(streamsConfig).toProperties(
            clientId = "${streamsConfig.applicationId}-consumer-${topic.name}",
            groupId = "${streamsConfig.applicationId}-${topic.name}-$groupIdSuffix",
            offsetResetPolicy = offsetResetPolicy
        )
        return KafkaConsumer(properties, topic.keySerde.deserializer(), topic.valueSerde.deserializer())
    }
}
