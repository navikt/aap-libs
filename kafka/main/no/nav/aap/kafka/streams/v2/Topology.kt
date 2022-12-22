package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.Topic
import no.nav.aap.kafka.streams.extension.consume
import no.nav.aap.kafka.streams.extension.filterNotNull
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.KStream

class Topology internal constructor() {
    private val builder = StreamsBuilder()

    fun <V : Any> consume(topic: Topic<V>): ConsumedKStream<V> {
        val consumeNotNulls: KStream<String, V> = builder
            .consume(topic)
            .filterNotNull("filter-tombstone-${topic.name}")

        return ConsumedKStream(topic, consumeNotNulls)
    }

    internal fun build(): org.apache.kafka.streams.Topology = builder.build()
}

fun topology(init: Topology.() -> Unit): Topology = Topology().apply(init)