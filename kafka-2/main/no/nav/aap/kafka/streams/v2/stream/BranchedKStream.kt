package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.Branched

class BranchedKStream<T : Any> internal constructor(
    private val topic: Topic<T>,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, T>,
) {

    fun branch(
        predicate: (T) -> Boolean,
        consumed: (ConsumedKStream<T>) -> Unit
    ): BranchedKStream<T> {
        stream.branch(
            { _, value -> predicate(value) },
            Branched.withConsumer {
                consumed(ConsumedKStream(topic, it))
            }
        )
        return this
    }

    fun default(consumed: (ConsumedKStream<T>) -> Unit) {
        stream.defaultBranch(
            Branched.withConsumer {
                consumed(ConsumedKStream(topic, it))
            }
        )
    }
}

class BranchedMappedKStream<T : Any> internal constructor(
    private val sourceTopicName: String,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, T>,
) {
    fun branch(
        predicate: (T) -> Boolean,
        consumed: (MappedKStream<T>) -> Unit
    ): BranchedMappedKStream<T> {
        stream.branch(
            { _, value -> predicate(value) },
            Branched.withConsumer {
                consumed(MappedKStream(sourceTopicName, it))
            }
        )
        return this
    }

    fun default(consumed: (MappedKStream<T>) -> Unit) {
        stream.defaultBranch(
            Branched.withConsumer {
                consumed(MappedKStream(sourceTopicName, it))
            }
        )
    }
}
