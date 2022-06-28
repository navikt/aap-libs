package no.nav.aap.kafka

import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.KStreamsConfig
import no.nav.aap.kafka.streams.KafkaStreams
import no.nav.aap.kafka.streams.Topic
import no.nav.aap.kafka.vanilla.KafkaConfig
import org.apache.kafka.clients.consumer.clientId
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals

class KafkaFactoryTest {

    @Test
    fun `can create consumer`() {
        val streamsConfig = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092")
        val config = KafkaConfig.copyFrom(streamsConfig)

        val consumer = KafkaStreams.createConsumer(config, Topic("topicA", JsonSerde.jackson()))
        val groupId = consumer.groupMetadata().groupId()
        assertEquals("topicA-1", groupId)
    }

    @Test
    fun `can create producer`() {
        val streamsConfig = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092")
        val config = KafkaConfig.copyFrom(streamsConfig)
        val producer = KafkaStreams.createProducer(config, Topic("topicB", JsonSerde.jackson()))
        producer.close(Duration.ofMillis(0))
    }

    @Test
    fun `can create multiple consumers`() {
        val streamsConfig = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092")
        val config = KafkaConfig.copyFrom(streamsConfig)
        val consumer1 = KafkaStreams.createConsumer(config, Topic("topicC", JsonSerde.jackson()))
        val consumer2 = KafkaStreams.createConsumer(config, Topic("topicD", JsonSerde.jackson()))
        assertEquals("topicC-1", consumer1.groupMetadata().groupId())
        assertEquals("topicD-1", consumer2.groupMetadata().groupId())
        assertEquals("consumer-topicC", consumer1.clientId())
        assertEquals("consumer-topicD", consumer2.clientId())
    }


    @Test
    fun `can create multiple producers`() {
        val streamsConfig = KStreamsConfig(applicationId = "test-app", brokers = "localhost:9092")
        val config = KafkaConfig.copyFrom(streamsConfig)
        val producer1 = KafkaStreams.createProducer(config, Topic("topicE", JsonSerde.jackson()))
        val producer2 = KafkaStreams.createProducer(config, Topic("topicF", JsonSerde.jackson()))
        producer1.close(Duration.ofMillis(0))
        producer2.close(Duration.ofMillis(0))
    }
}
