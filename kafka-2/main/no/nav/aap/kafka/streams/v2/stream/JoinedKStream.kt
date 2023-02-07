package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.KStreamPair
import no.nav.aap.kafka.streams.v2.KeyValue
import no.nav.aap.kafka.streams.v2.NullableKStreamPair
import no.nav.aap.kafka.streams.v2.extension.log
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import org.apache.kafka.streams.kstream.KStream

class JoinedKStream<L, R> internal constructor(
    private val sourceTopicName: String,
    private val stream: KStream<String, KStreamPair<L, R>>,
) {
    fun <LR : Any> map(joinFunction: (L, R) -> LR): MappedKStream<LR> {
        val fusedStream = stream.mapValues { pair -> joinFunction(pair.left, pair.right) }
        return MappedKStream(sourceTopicName, fusedStream)
    }

    fun filter(lambda: (KStreamPair<L, R>) -> Boolean): JoinedKStream<L, R> {
        val stream = stream.filter { _, value -> lambda(value) }
        return JoinedKStream(sourceTopicName, stream)
    }

    fun branch(
        predicate: (KStreamPair<L, R>) -> Boolean,
        consumed: (MappedKStream<KStreamPair<L, R>>) -> Unit,
    ): BranchedMappedKStream<KStreamPair<L, R>> {
        return BranchedMappedKStream(sourceTopicName, stream.split())
            .branch(predicate, consumed)
    }
}

class LeftJoinedKStream<L, R> internal constructor(
    private val sourceTopicName: String,
    private val stream: KStream<String, NullableKStreamPair<L, R>>,
) {
    fun <LR : Any> map(joinFunction: (L, R?) -> LR): MappedKStream<LR> {
        val fusedStream = stream.mapValues { pair -> joinFunction(pair.left, pair.right) }
        return MappedKStream(sourceTopicName, fusedStream)
    }

    fun <LR : Any> mapKeyValue(mapper: (String, L, R?) -> KeyValue<String, LR>): MappedKStream<LR> {
        val fusedStream =
            stream.map { key, (leftValue, rightValue) -> mapper(key, leftValue, rightValue).toInternalKeyValue() }
        return MappedKStream(sourceTopicName, fusedStream)
    }

    fun filter(lambda: (NullableKStreamPair<L, R>) -> Boolean): LeftJoinedKStream<L, R> {
        val stream = stream.filter { _, value -> lambda(value) }
        return LeftJoinedKStream(sourceTopicName, stream)
    }

    fun branch(
        predicate: (NullableKStreamPair<L, R>) -> Boolean,
        consumed: (MappedKStream<NullableKStreamPair<L, R>>) -> Unit,
    ): BranchedMappedKStream<NullableKStreamPair<L, R>> {
        return BranchedMappedKStream(sourceTopicName, stream.split())
            .branch(predicate, consumed)
    }

    fun log(level: LogLevel = LogLevel.INFO, keyValue: (String, L, R?) -> Any): LeftJoinedKStream<L, R> {
        stream.log(level, keyValue)
        return this
    }
}
