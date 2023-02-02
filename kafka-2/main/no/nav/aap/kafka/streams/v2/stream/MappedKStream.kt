package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import no.nav.aap.kafka.streams.v2.logger.log
import org.apache.kafka.streams.kstream.KStream

class MappedKStream<L, LR : Any> internal constructor(
    private val source: Topic<L>,
    private val stream: KStream<String, LR>,
) {
    fun produce(topic: Topic<LR>, logValues: Boolean = false) {
        stream.produceToTopic(
            topic = topic,
            named = "produced-${topic.name}-from-${source.name}",
            logValues = logValues,
        )
    }

    fun <V : Any> map(mapper: (LR) -> V): MappedKStream<L, V> {
        val mappedStream = stream.mapValues { lr -> mapper(lr) }
        return MappedKStream(source, mappedStream)
    }

    fun <V : Any> map(mapper: (key: String, value: LR) -> V): MappedKStream<L, V> {
        val fusedStream = stream.mapValues { key, value -> mapper(key, value) }
        return MappedKStream(source, fusedStream)
    }

    fun filter(lambda: (LR) -> Boolean): MappedKStream<L, LR> {
        val stream = stream.filter { _, value -> lambda(value) }
        return MappedKStream(source, stream)
    }

    fun branch(predicate: (LR) -> Boolean, consumed: (MappedKStream<L, LR>) -> Unit): BranchedKStream<L, LR> {
        return BranchedKStream(source, stream.split())
            .branch(predicate, consumed)
    }

    fun log(level: LogLevel = LogLevel.INFO, keyValue: (String, LR) -> Any): MappedKStream<L, LR> {
        stream.log(level, keyValue)
        return this
    }
}
