package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.Table
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.extension.log
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import no.nav.aap.kafka.streams.v2.processor.Processor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateProcessor.Companion.addProcessor
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named

class MappedKStream<T : Any> internal constructor(
    private val sourceTopicName: String,
    private val stream: KStream<String, T>,
    private val namedSupplier: () -> String,
) {
    fun produce(table: Table<T>, logValues: Boolean = false): KTable<T> {
        val internalKTable = stream.produceToTable(table, logValues)
        return KTable(table, internalKTable)
    }

    fun produce(topic: Topic<T>, logValues: Boolean = false) {
        val named = "produced-${topic.name}-${namedSupplier()}"
        stream.produceToTopic(topic, named, logValues)
    }

    fun <R : Any> map(mapper: (T) -> R): MappedKStream<R> {
        val mappedStream = stream.mapValues { lr -> mapper(lr) }
        return MappedKStream(sourceTopicName, mappedStream, namedSupplier)
    }

    fun <R : Any> map(mapper: (key: String, value: T) -> R): MappedKStream<R> {
        val fusedStream = stream.mapValues { key, value -> mapper(key, value) }
        return MappedKStream(sourceTopicName, fusedStream, namedSupplier)
    }

    fun rekey(mapper: (value: T) -> String): MappedKStream<T> {
        val fusedStream = stream.selectKey { _, value -> mapper(value) }
        return MappedKStream(sourceTopicName, fusedStream, namedSupplier)
    }

    fun filter(lambda: (T) -> Boolean): MappedKStream<T> {
        val filteredStream = stream.filter { _, value -> lambda(value) }
        return MappedKStream(sourceTopicName, filteredStream, namedSupplier)
    }

    fun branch(predicate: (T) -> Boolean, consumed: (MappedKStream<T>) -> Unit): BranchedMappedKStream<T> {
        val named = Named.`as`("split-${namedSupplier()}")
        val branchedStream = stream.split(named)
        return BranchedMappedKStream(sourceTopicName, branchedStream, namedSupplier).branch(predicate, consumed)
    }

    fun log(level: LogLevel = LogLevel.INFO, keyValue: (String, T) -> Any): MappedKStream<T> {
        val loggedStream = stream.log(level, keyValue)
        return MappedKStream(sourceTopicName, loggedStream, namedSupplier)
    }

    fun <U : Any> processor(processor: Processor<T, U>): MappedKStream<U> {
        val processedStream = stream.addProcessor(processor)
        return MappedKStream(sourceTopicName, processedStream, namedSupplier)
    }

    fun <TABLE, U : Any> processor(processor: StateProcessor<TABLE, T, U>): MappedKStream<U> {
        val processedStream = stream.addProcessor(processor)
        return MappedKStream(sourceTopicName, processedStream, namedSupplier)
    }

    fun forEach(mapper: (key: String, value: T) -> Unit) = stream.foreach(mapper)
}
