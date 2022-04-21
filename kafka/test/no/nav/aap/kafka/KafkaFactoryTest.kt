package no.nav.aap.kafka

import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.Test
import kotlin.test.assertEquals

class KafkaFactoryTest {

    @Test
    fun consumer() {
        val factory = KafkaFactory(defaultKafkaTestConfig.copy(credstorePsw = ""))
        val consumer = factory.createConsumer(Topic("topic", JsonSerde.jackson()))
        val groupId = consumer.groupMetadata().groupId()
        assertEquals("topic-1", groupId)
    }

}