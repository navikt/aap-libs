package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.concurrency.Bufferable
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.concurrency.RaceConditionBuffer
import no.nav.aap.kafka.streams.v2.logger.Log
import no.nav.aap.kafka.streams.v2.processor.LogProduceTopicProcessor
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
    fun produce(topic: Topic<T>, logValues: Boolean = false) {
        val named = "produced-${topic.name}-${namedSupplier()}"
        stream.produceToTopic(topic, named, logValues)
    }

    fun <U : Bufferable<U>> produce(
        topic: Topic<U>,
        buffer: RaceConditionBuffer<U>,
        logValues: Boolean = false,
        lambda: (T) -> U,
    ) {
        val named = "produced-bufferable-${topic.name}-${namedSupplier()}"

        stream
            .mapValues { key, value ->
                lambda(value).also {
                    buffer.lagre(key, it)
                }
            }
            .addProcessor(
                LogProduceTopicProcessor(
                    named = "log-${named}",
                    topic = topic,
                    logValue = logValues,
                )
            )
            .to(topic.name, topic.produced(named))
    }

    fun <R : Any> map(mapper: (T) -> R): MappedKStream<R> {
        val mappedStream = stream.mapValues { lr -> mapper(lr) }
        return MappedKStream(sourceTopicName, mappedStream, namedSupplier)
    }

    fun <R : Any> map(mapper: (key: String, value: T) -> R): MappedKStream<R> {
        val mappedStream = stream.mapValues { key, value -> mapper(key, value) }
        return MappedKStream(sourceTopicName, mappedStream, namedSupplier)
    }

    fun <R : Any> flatMap(mapper: (value: T) -> Iterable<R>): MappedKStream<R> {
        val flattenedStream = stream.flatMapValues { _, value -> mapper(value) }
        return MappedKStream(sourceTopicName, flattenedStream, namedSupplier)
    }

    fun rekey(mapper: (value: T) -> String): MappedKStream<T> {
        val rekeyedStream = stream.selectKey { _, value -> mapper(value) }
        return MappedKStream(sourceTopicName, rekeyedStream, namedSupplier)
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

    fun secureLog(logger: Log.(value: T) -> Unit): MappedKStream<T> {
        val loggedStream = stream.peek { _, value -> logger.invoke(Log.secure, value) }
        return MappedKStream(sourceTopicName, loggedStream, namedSupplier)
    }

    fun secureLogWithKey(log: Log.(key: String, value: T) -> Unit): MappedKStream<T> {
        val loggedStream = stream.peek { key, value -> log.invoke(Log.secure, key, value) }
        return MappedKStream(sourceTopicName, loggedStream, namedSupplier)
    }

    fun <U : Any> processor(processor: Processor<T, U>): MappedKStream<U> {
        val processedStream = stream.addProcessor(processor)
        return MappedKStream(sourceTopicName, processedStream, namedSupplier)
    }

    fun <TABLE : Any, U : Any> processor(processor: StateProcessor<TABLE, T, U>): MappedKStream<U> {
        val processedStream = stream.addProcessor(processor)
        return MappedKStream(sourceTopicName, processedStream, namedSupplier)
    }

    fun forEach(mapper: (key: String, value: T) -> Unit) {
        val named = Named.`as`("foreach-${namedSupplier()}")
        stream.foreach(mapper, named)
    }
}
