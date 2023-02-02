package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.Branched

class BranchedKStream<T, S : Any> internal constructor(
    private val topic: Topic<T>,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, S>,
) {

    fun branch(
        predicate: (S) -> Boolean,
        consumed: (MappedKStream<T, S>) -> Unit
    ): BranchedKStream<T, S> {
        stream.branch(
            { _, value -> predicate(value) },
            Branched.withConsumer {
                consumed(MappedKStream(topic, it))
            }
        )
        return this
    }
}
