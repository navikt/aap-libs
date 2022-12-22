package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.Topic
import no.nav.aap.kafka.streams.extension.produce
import org.apache.kafka.streams.kstream.KStream

class MappedKStream<L, LR : Any>(
    private val source: Topic<L>,
    private val kstream: KStream<String, LR>,
) {
    fun produce(topic: Topic<LR>) {
        kstream.produce(topic, "produced-to${topic.name}-from-${source.name}")
    }

    fun <V : Any> map(mapFunction: (LR) -> V): MappedKStream<L, V> {
        val mappedStream = kstream.mapValues { lr -> mapFunction(lr) }
        return MappedKStream(source, mappedStream)
    }

    fun filter(lambda: (LR) -> Boolean): MappedKStream<L, LR> {
        val stream = kstream.filter { _, value -> lambda(value) }
        return MappedKStream(source, stream)
    }
}