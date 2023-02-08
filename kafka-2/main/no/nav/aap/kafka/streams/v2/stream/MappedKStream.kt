package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.KeyValue
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.extension.log
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import no.nav.aap.kafka.streams.v2.processor.Processor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateProcessor.Companion.addProcessor
import org.apache.kafka.streams.kstream.KStream

class MappedKStream<T : Any> internal constructor(
    private val sourceTopicName: String,
    private val stream: KStream<String, T>,
) {
    fun produce(topic: Topic<T>, logValues: Boolean = false) {
        stream.produceToTopic(
            topic = topic,
            named = "produced-${topic.name}-from-$sourceTopicName",
            logValues = logValues,
        )
    }

    fun <R : Any> map(mapper: (T) -> R): MappedKStream<R> {
        val mappedStream = stream.mapValues { lr -> mapper(lr) }
        return MappedKStream(sourceTopicName, mappedStream)
    }

    fun <R : Any> map(mapper: (key: String, value: T) -> R): MappedKStream<R> {
        val fusedStream = stream.mapValues { key, value -> mapper(key, value) }
        return MappedKStream(sourceTopicName, fusedStream)
    }

    fun <R : Any> mapKeyAndValue(mapper: (key: String, value: T) -> KeyValue<String, R>): MappedKStream<R> {
        val fusedStream = stream.map { key, value -> mapper(key, value).toInternalKeyValue() }
        return MappedKStream(sourceTopicName, fusedStream)
    }

    fun filter(lambda: (T) -> Boolean): MappedKStream<T> {
        val stream = stream.filter { _, value -> lambda(value) }
        return MappedKStream(sourceTopicName, stream)
    }

    fun branch(predicate: (T) -> Boolean, consumed: (MappedKStream<T>) -> Unit): BranchedMappedKStream<T> =
        BranchedMappedKStream(
            sourceTopicName = sourceTopicName,
            stream = stream.split()
        ).branch(
            predicate = predicate,
            consumed = consumed,
        )

    fun log(level: LogLevel = LogLevel.INFO, keyValue: (String, T) -> Any): MappedKStream<T> {
        stream.log(level, keyValue)
        return this
    }

    fun <U : Any> processor(processor: Processor<T, U>): MappedKStream<U> =
        MappedKStream(
            sourceTopicName = sourceTopicName,
            stream = stream.addProcessor(processor)
        )

    fun <TABLE, U : Any> processor(processor: StateProcessor<TABLE, T, U>): MappedKStream<U> =
        MappedKStream(
            sourceTopicName = sourceTopicName,
            stream = stream.addProcessor(processor)
        )
}
