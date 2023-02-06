package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import no.nav.aap.kafka.streams.v2.logger.log
import no.nav.aap.kafka.streams.v2.processor.KProcessor
import no.nav.aap.kafka.streams.v2.processor.KProcessor.Companion.addProcessor
import no.nav.aap.kafka.streams.v2.processor.KStoreProcessor
import no.nav.aap.kafka.streams.v2.processor.KStoreProcessor.Companion.addProcessor
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

    fun <U : Any> processor(processor: KProcessor<T, U>): MappedKStream<U> =
        MappedKStream(
            sourceTopicName = sourceTopicName,
            stream = stream.addProcessor(processor)
        )

    fun <U : Any> processor(processor: KStoreProcessor<T, U>): MappedKStream<U> =
        MappedKStream(
            sourceTopicName = sourceTopicName,
            stream = stream.addProcessor(processor)
        )
}
