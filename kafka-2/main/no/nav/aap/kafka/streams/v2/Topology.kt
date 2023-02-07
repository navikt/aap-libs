package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.v2.extension.consume
import no.nav.aap.kafka.streams.v2.extension.skipTombstone
import no.nav.aap.kafka.streams.v2.processor.state.StateScheduleProcessor
import no.nav.aap.kafka.streams.v2.stream.ConsumedKStream
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream

class Topology internal constructor() {
    private val builder = StreamsBuilder()

    fun <T : Any> consume(topic: Topic<T>, logValue: Boolean = false): ConsumedKStream<T> {
        val consumeNotNulls: KStream<String, T> = builder
            .consume(topic, logValue)
            .skipTombstone(topic)

        return ConsumedKStream(topic, consumeNotNulls)
    }

    fun <T> schedule(scheduler: StateScheduleProcessor<T>) {
        StateScheduleProcessor.initInternalProcessor(scheduler)
    }

    internal fun build(): org.apache.kafka.streams.Topology = builder.build()
}

fun topology(init: Topology.() -> Unit): Topology = Topology().apply(init)
