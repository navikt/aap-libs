package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.*
import no.nav.aap.kafka.streams.v2.extension.filterNotNull
import no.nav.aap.kafka.streams.v2.extension.join
import no.nav.aap.kafka.streams.v2.extension.leftJoin
import no.nav.aap.kafka.streams.v2.extension.log
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import no.nav.aap.kafka.streams.v2.processor.Processor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateProcessor.Companion.addProcessor
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Repartitioned

class ConsumedKStream<T : Any> internal constructor(
    private val topic: Topic<T>,
    private val stream: KStream<String, T>,
) {
    fun produce(table: Table<T>, logValues: Boolean = false): KTable<T> =
        KTable(
            table = table,
            internalTable = stream.produceToTable(table, logValues)
        )

    fun produce(destination: Topic<T>, logValues: Boolean = false) {
        stream.produceToTopic(
            topic = destination,
            named = "produced-${destination.name}-from-${topic.name}",
            logValues = logValues,
        )
    }

    fun rekey(selectKeyFromValue: (T) -> String): ConsumedKStream<T> {
        val stream = stream.selectKey { _, value -> selectKeyFromValue(value) }
        return ConsumedKStream(topic, stream)
    }

    fun filter(lambda: (T) -> Boolean): ConsumedKStream<T> {
        val stream = stream.filter { _, value -> lambda(value) }
        return ConsumedKStream(topic, stream)
    }

    fun <R : Any> map(mapper: (value: T) -> R): MappedKStream<R> {
        val fusedStream = stream.mapValues { value -> mapper(value) }
        return MappedKStream(topic.name, fusedStream)
    }

    fun <R : Any> map(mapper: (key: String, value: T) -> R): MappedKStream<R> {
        val fusedStream = stream.mapValues { key, value -> mapper(key, value) }
        return MappedKStream(topic.name, fusedStream)
    }

    fun <R> mapNotNull(mapper: (key: String, value: T) -> R): MappedKStream<R & Any> =
        MappedKStream(
            sourceTopicName = topic.name,
            stream = stream
                .mapValues { key, value -> mapper(key, value) }
                .filterNotNull()
        )

    fun flatMapPreserveType(mapper: (key: String, value: T) -> Iterable<T>): ConsumedKStream<T> {
        val fusedStream = stream.flatMapValues { key, value -> mapper(key, value) }
        return ConsumedKStream(topic, fusedStream)
    }

    fun flatMapKeyAndValuePreserveType(mapper: (key: String, value: T) -> Iterable<KeyValue<String, T>>): ConsumedKStream<T> {
        val fusedStream =
            stream.flatMap { key, value -> mapper(key, value).map(KeyValue<String, T>::toInternalKeyValue) }
        return ConsumedKStream(topic, fusedStream)
    }

    fun <R : Any> flatMap(mapper: (key: String, value: T) -> Iterable<R>): MappedKStream<R> {
        val fusedStream = stream.flatMapValues { key, value -> mapper(key, value) }
        return MappedKStream(topic.name, fusedStream)
    }

    fun <R : Any> flatMapKeyAndValue(mapper: (key: String, value: T) -> Iterable<KeyValue<String, R>>): MappedKStream<R> {
        val fusedStream =
            stream.flatMap { key, value -> mapper(key, value).map(KeyValue<String, R>::toInternalKeyValue) }
        return MappedKStream(topic.name, fusedStream)
    }

    fun <R : Any> mapKeyAndValue(mapper: (key: String, value: T) -> KeyValue<String, R>): MappedKStream<R> {
        val fusedStream = stream.map { key, value -> mapper(key, value).toInternalKeyValue() }
        return MappedKStream(topic.name, fusedStream)
    }

    fun <U : Any> joinWith(table: KTable<U>): JoinedKStream<T, U> =
        JoinedKStream(
            sourceTopicName = topic.name,
            stream = stream.join(
                left = topic,
                right = table,
                joiner = ::KStreamPair,
            )
        )

    fun <U : Any> leftJoinWith(table: KTable<U>): LeftJoinedKStream<T, U> =
        LeftJoinedKStream(
            sourceTopicName = topic.name,
            stream = stream.leftJoin(
                left = topic,
                right = table,
                joiner = ::NullableKStreamPair,
            )
        )

    fun branch(predicate: (T) -> Boolean, consumed: (ConsumedKStream<T>) -> Unit): BranchedKStream<T> {
        return BranchedKStream(topic, stream.split()).branch(predicate, consumed)
    }

    fun log(level: LogLevel = LogLevel.INFO, keyValue: (String, T) -> Any): ConsumedKStream<T> {
        stream.log(level, keyValue)
        return this
    }

    fun repartition(partitions: Int = 12): ConsumedKStream<T> {
        val repartition = Repartitioned.with(topic.keySerde, topic.valueSerde).withNumberOfPartitions(partitions)
        return ConsumedKStream(topic, stream.repartition(repartition))
    }

    fun <U : Any> processor(processor: Processor<T, U>): MappedKStream<U> =
        MappedKStream(
            sourceTopicName = topic.name,
            stream = stream.addProcessor(processor)
        )

    fun <TABLE, U: Any> processor(processor: StateProcessor<TABLE, T, U>): MappedKStream<U> =
        MappedKStream(
            sourceTopicName = topic.name,
            stream = stream.addProcessor(processor)
        )
}
