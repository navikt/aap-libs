package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topics
import no.nav.aap.kafka.streams.v2.produce
import no.nav.aap.kafka.streams.v2.kafkaWithTopology
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class ConsumedStreamTest {

    @Test
    fun `map with metadata`() {
        val kafka = kafkaWithTopology {
            consume(Topics.A)
                .mapWithMetadata { value, metadata -> value + metadata.topic }
                .produce(Topics.B)
        }

        kafka.inputTopic(Topics.A).produce("1", "hello:")

        val result = kafka.outputTopic(Topics.B).readKeyValuesToMap()

        assertEquals(1, result.size)
        assertEquals("hello:A", result["1"])
    }
}
