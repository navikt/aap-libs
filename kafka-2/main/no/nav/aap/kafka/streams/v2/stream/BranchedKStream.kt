package no.nav.aap.kafka.streams.v2.stream

import org.apache.kafka.streams.kstream.Branched

class BranchedKStream<T : Any> internal constructor(
    private val sourceTopicName: String,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, T>,
) {

    fun branch(
        predicate: (T) -> Boolean,
        consumed: (MappedKStream<T>) -> Unit
    ): BranchedKStream<T> {
        stream.branch(
            { _, value -> predicate(value) },
            Branched.withConsumer {
                consumed(MappedKStream(sourceTopicName, it))
            }
        )
        return this
    }
}
