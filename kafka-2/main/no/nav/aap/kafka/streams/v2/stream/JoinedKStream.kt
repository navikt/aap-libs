package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.KStreamPair
import no.nav.aap.kafka.streams.v2.KeyValue
import no.nav.aap.kafka.streams.v2.NullableKStreamPair
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import no.nav.aap.kafka.streams.v2.logger.log
import org.apache.kafka.streams.kstream.KStream

class JoinedKStream<L, R>(
    private val left: Topic<L>, // source
    private val right: Topic<R>,
    private val stream: KStream<String, KStreamPair<L, R>>,
) {
    fun <LR : Any> map(joinFunction: (L, R) -> LR): MappedKStream<L, LR> {
        val fusedStream = stream.mapValues { pair -> joinFunction(pair.left, pair.right) }
        return MappedKStream(left, fusedStream)
    }

    fun filter(lambda: (KStreamPair<L, R>) -> Boolean): JoinedKStream<L, R> {
        val stream = stream.filter { _, value -> lambda(value) }
        return JoinedKStream(left, right, stream)
    }
}

class LeftJoinedKStream<L, R>(
    private val left: Topic<L>, // source
    private val right: Topic<R>,
    private val stream: KStream<String, NullableKStreamPair<L, R>>,
) {
    fun <LR : Any> map(joinFunction: (L, R?) -> LR): MappedKStream<L, LR> {
        val fusedStream = stream.mapValues { pair -> joinFunction(pair.left, pair.right) }
        return MappedKStream(left, fusedStream)
    }

    fun <LR : Any> mapKeyValue(mapper: (String, L, R?) -> KeyValue<String, LR>): MappedKStream<L, LR> {
        val fusedStream = stream.map { key, (leftValue, rightValue) -> mapper(key, leftValue, rightValue).toInternalKeyValue() }
        return MappedKStream(left, fusedStream)
    }

    fun filter(lambda: (NullableKStreamPair<L, R>) -> Boolean): LeftJoinedKStream<L, R> {
        val stream = stream.filter { _, value -> lambda(value) }
        return LeftJoinedKStream(left, right, stream)
    }

    fun log(level: LogLevel = LogLevel.INFO, keyValue: (String, L, R?) -> Any): LeftJoinedKStream<L, R> {
        stream.log(level, keyValue)
        return this
    }
}
