package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.Branched

class BranchedKStream<L : Any>(
    private val topic: Topic<L>,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, L>,
) {

    fun branch(
        predicate: (L) -> Boolean,
        consumed: (ConsumedKStream<L>) -> Unit
    ): BranchedKStream<L> {
        stream.branch(
            { _, value -> predicate(value) },
            Branched.withConsumer {
                consumed(ConsumedKStream(topic, it))
            }
        )
        return this
    }
}
