package no.nav.aap.kafka.streams.test

import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic

data class TestTopic<V: Any>(
    private val input: TestInputTopic<String, V>,
    private val output: TestOutputTopic<String, V>
) {
    fun produce(key: String, value: () -> V) = input.pipeInput(key, value())

    fun tombstone(key: String) = input.pipeInput(key, null)

    fun assertThat() = output.readAndAssert()

    fun readValue() = output.readValue()
}
