package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.streams.kstream.KStream

class JoinedKStream<L, R>(
    private val left: Topic<L>, // source
    private val right: Topic<R>,
    private val kstream: KStream<String, KStreamPair<L, R>>,
) {
    fun <LR : Any> map(joinFunction: (L, R) -> LR): MappedKStream<L, LR> {
        val fusedStream = kstream.mapValues { pair -> joinFunction(pair.left, pair.right) }
        return MappedKStream(left, fusedStream)
    }

    fun filter(lambda: (KStreamPair<L, R>) -> Boolean): JoinedKStream<L, R> {
        val stream = kstream.filter { _, value -> lambda(value) }
        return JoinedKStream(left, right, stream)
    }
}

class LeftJoinedKStream<L, R>(
    private val left: Topic<L>, // source
    private val right: Topic<R & Any>,
    private val kstream: KStream<String, KStreamPair<L, R?>>,
) {
    fun <LR : Any> map(joinFunction: (L, R?) -> LR): MappedKStream<L, LR> {
        val fusedStream = kstream.mapValues { pair -> joinFunction(pair.left, pair.right) }
        return MappedKStream(left, fusedStream)
    }

    fun filter(lambda: (KStreamPair<L, R?>) -> Boolean): LeftJoinedKStream<L, R> {
        val stream = kstream.filter { _, value -> lambda(value) }
        return LeftJoinedKStream(left, right, stream)
    }
}