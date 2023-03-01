package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.concurrency.Bufferable
import no.nav.aap.kafka.streams.v2.*
import no.nav.aap.kafka.streams.v2.concurrency.RaceConditionBuffer
import no.nav.aap.kafka.streams.v2.extension.filterNotNull
import no.nav.aap.kafka.streams.v2.extension.join
import no.nav.aap.kafka.streams.v2.extension.leftJoin
import no.nav.aap.kafka.streams.v2.logger.Log
import no.nav.aap.kafka.streams.v2.processor.Processor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateProcessor.Companion.addProcessor
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Repartitioned

class ConsumedKStream<T : Any> internal constructor(
    private val topic: Topic<T>,
    private val stream: KStream<String, T>,
    private val namedSupplier: () -> String
) {
    fun produce(table: Table<T>, logValues: Boolean = false): KTable<T> {
        val internalKTable = stream.produceToTable(table, logValues)
        return KTable(table, internalKTable)
    }

    fun produce(destination: Topic<T>, logValues: Boolean = false) {
        val named = "produced-${destination.name}-${namedSupplier()}"
        stream.produceToTopic(destination, named, logValues)
    }

    fun rekey(selectKeyFromValue: (T) -> String): ConsumedKStream<T> {
        val rekeyedStream = stream.selectKey { _, value -> selectKeyFromValue(value) }
        return ConsumedKStream(topic, rekeyedStream, namedSupplier)
    }

    fun filter(lambda: (T) -> Boolean): ConsumedKStream<T> {
        val filteredStream = stream.filter { _, value -> lambda(value) }
        return ConsumedKStream(topic, filteredStream, namedSupplier)
    }

    fun filterKey(lambda: (String) -> Boolean): ConsumedKStream<T> {
        val filteredStream = stream.filter { key, _ -> lambda(key) }
        return ConsumedKStream(topic, filteredStream, namedSupplier)
    }

    fun <R : Any> map(mapper: (value: T) -> R): MappedKStream<R> {
        val mappedStream = stream.mapValues { value -> mapper(value) }
        return MappedKStream(topic.name, mappedStream, namedSupplier)
    }

    fun <R : Any> map(mapper: (key: String, value: T) -> R): MappedKStream<R> {
        val mappedStream = stream.mapValues { key, value -> mapper(key, value) }
        return MappedKStream(topic.name, mappedStream, namedSupplier)
    }

    fun <R> mapNotNull(mapper: (key: String, value: T) -> R): MappedKStream<R & Any> {
        val valuedStream = stream.mapValues { key, value -> mapper(key, value) }.filterNotNull()
        return MappedKStream(topic.name, valuedStream, namedSupplier)
    }

    fun flatMapPreserveType(mapper: (key: String, value: T) -> Iterable<T>): ConsumedKStream<T> {
        val fusedStream = stream.flatMapValues { key, value -> mapper(key, value) }
        return ConsumedKStream(topic, fusedStream, namedSupplier)
    }

    fun flatMapKeyAndValuePreserveType(mapper: (key: String, value: T) -> Iterable<KeyValue<String, T>>): ConsumedKStream<T> {
        val fusedStream = stream.flatMap { key, value -> mapper(key, value).map { it.toInternalKeyValue() } }
        return ConsumedKStream(topic, fusedStream, namedSupplier)
    }

    fun <R : Any> flatMap(mapper: (key: String, value: T) -> Iterable<R>): MappedKStream<R> {
        val fusedStream = stream.flatMapValues { key, value -> mapper(key, value) }
        return MappedKStream(topic.name, fusedStream, namedSupplier)
    }

    fun <R : Any> flatMapKeyAndValue(mapper: (key: String, value: T) -> Iterable<KeyValue<String, R>>): MappedKStream<R> {
        val fusedStream = stream.flatMap { key, value -> mapper(key, value).map { it.toInternalKeyValue() } }
        return MappedKStream(topic.name, fusedStream, namedSupplier)
    }

    fun <R : Any> mapKeyAndValue(mapper: (key: String, value: T) -> KeyValue<String, R>): MappedKStream<R> {
        val fusedStream = stream.map { key, value -> mapper(key, value).toInternalKeyValue() }
        return MappedKStream(topic.name, fusedStream, namedSupplier)
    }

    fun <U : Any> joinWith(ktable: KTable<U>): JoinedKStream<T, U> {
        val joinedStream = stream.join(topic, ktable, ::KStreamPair)
        val named = { "${topic.name}-join-${ktable.table.sourceTopic.name}" }
        return JoinedKStream(topic.name, joinedStream, named)
    }

    fun <U : Bufferable<U>> joinWith(ktable: KTable<U>, buffer: RaceConditionBuffer<U>): JoinedKStream<T, U> {
        fun joiner(key: String, left: T, right: U): KStreamPair<T, U> {
            return KStreamPair(left, buffer.velgNyeste(key, right))
        }

        val joinedStream = stream.join(topic, ktable, ::joiner)
        val named = { "${topic.name}-buffered-join-${ktable.table.sourceTopic.name}" }
        return JoinedKStream(topic.name, joinedStream, named)
    }

    fun <U : Any> leftJoinWith(ktable: KTable<U>): LeftJoinedKStream<T, U> {
        val joinedStream = stream.leftJoin(topic, ktable, ::NullableKStreamPair)
        val named = { "${topic.name}-left-join-${ktable.table.sourceTopic.name}" }
        return LeftJoinedKStream(topic.name, joinedStream, named)
    }

    fun <U : Bufferable<U>> leftJoinWith(ktable: KTable<U>, buffer: RaceConditionBuffer<U>): LeftJoinedKStream<T, U> {
        fun joiner(key: String, left: T, right: U?): NullableKStreamPair<T, U> {
            return NullableKStreamPair(left, buffer.velgNyesteNullable(key, right))
        }

        val joinedStream = stream.leftJoin(topic, ktable, ::joiner)
        val named = { "${topic.name}-buffered-left-join-${ktable.table.sourceTopic.name}" }
        return LeftJoinedKStream(topic.name, joinedStream, named)
    }

    fun branch(predicate: (T) -> Boolean, consumed: (ConsumedKStream<T>) -> Unit): BranchedKStream<T> {
        val splittedStream = stream.split(Named.`as`("split-${namedSupplier()}"))
        return BranchedKStream(topic, splittedStream, namedSupplier).branch(predicate, consumed)
    }

    fun secureLog(log: Log.(value: T) -> Unit): ConsumedKStream<T> {
        val loggedStream = stream.peek { _, value -> log.invoke(Log.secure, value) }
        return ConsumedKStream(topic, loggedStream, namedSupplier)
    }

    fun secureLogWithKey(log: Log.(key: String, value: T) -> Unit): ConsumedKStream<T> {
        val loggedStream = stream.peek { key, value -> log.invoke(Log.secure, key, value) }
        return ConsumedKStream(topic, loggedStream, namedSupplier)
    }

    fun repartition(partitions: Int = 12): ConsumedKStream<T> {
        val repartition = Repartitioned
            .with(topic.keySerde, topic.valueSerde)
            .withNumberOfPartitions(partitions)
            .withName(topic.name)
        return ConsumedKStream(topic, stream.repartition(repartition), namedSupplier)
    }

    fun <U : Any> processor(processor: Processor<T, U>): MappedKStream<U> {
        val processorStream = stream.addProcessor(processor)
        return MappedKStream(topic.name, processorStream, namedSupplier)
    }

    fun processor(processor: Processor<T, T>): ConsumedKStream<T> {
        val processorStream = stream.addProcessor(processor)
        return ConsumedKStream(topic, processorStream, namedSupplier)
    }

    fun <TABLE, U : Any> processor(processor: StateProcessor<TABLE, T, U>): MappedKStream<U> {
        val processorStream = stream.addProcessor(processor)
        return MappedKStream(topic.name, processorStream, namedSupplier)
    }

    fun forEach(mapper: (key: String, value: T) -> Unit) {
        val named = Named.`as`("foreach-${namedSupplier()}")
        stream.foreach(mapper, named)
    }
}
