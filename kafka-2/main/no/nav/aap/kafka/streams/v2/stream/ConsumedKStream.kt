package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.*
import no.nav.aap.kafka.streams.v2.extension.join
import no.nav.aap.kafka.streams.v2.extension.leftJoin
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import no.nav.aap.kafka.streams.v2.logger.log
import org.apache.kafka.streams.kstream.KStream

class ConsumedKStream<L : Any>(
    private val topic: Topic<L>,
    private val stream: KStream<String, L>,
) {
    fun produce(table: Table<L>, logValues: Boolean = false): KTable<L> =
        KTable(
            topic = topic,
            table = stream.produceToTable(table, logValues)
        )

    fun produce(destination: Topic<L>, logValues: Boolean = false) {
        stream.produceToTopic(
            topic = destination,
            named = "produced-${destination.name}-from-${topic.name}",
            logValues = logValues,
        )
    }

    fun rekey(selectKeyFromValue: (L) -> String): ConsumedKStream<L> {
        val stream = stream.selectKey { _, value -> selectKeyFromValue(value) }
        return ConsumedKStream(topic, stream)
    }

    fun filter(lambda: (L) -> Boolean): ConsumedKStream<L> {
        val stream = stream.filter { _, value -> lambda(value) }
        return ConsumedKStream(topic, stream)
    }

    fun <LR : Any> map(mapper: (L) -> LR): MappedKStream<L, LR> {
        val fusedStream = stream.mapValues { value -> mapper(value) }
        return MappedKStream(topic, fusedStream)
    }

    fun <LR : Any> mapKeyValue(mapper: (String, L) -> KeyValue<String, LR>): MappedKStream<L, LR> {
        val fusedStream = stream.map { key, value -> mapper(key, value).toInternalKeyValue() }
        return MappedKStream(topic, fusedStream)
    }

    fun <R : Any> joinWith(table: KTable<R>): JoinedKStream<L, R> =
        JoinedKStream(
            left = topic,
            right = table.topic,
            stream = stream.join(
                left = topic,
                right = table,
                joiner = ::KStreamPair,
            )
        )

    fun <R : Any> leftJoinWith(table: KTable<R>): LeftJoinedKStream<L, R> =
        LeftJoinedKStream(
            left = topic,
            right = table.topic,
            stream = stream.leftJoin(
                left = topic,
                right = table,
                joiner = ::NullableKStreamPair,
            )
        )

    fun log(level: LogLevel = LogLevel.INFO, keyValue: (String, L) -> Any): ConsumedKStream<L> {
        stream.log(level, keyValue)
        return this
    }
}
