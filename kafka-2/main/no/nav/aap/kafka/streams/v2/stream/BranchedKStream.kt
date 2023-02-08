package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.Branched

class BranchedKStream<T : Any> internal constructor(
    private val topic: Topic<T>,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, T>,
    private val named: String,
    ) {
    private var nextBranchNumber: Int = 1
        get() = field++

    fun branch(
        predicate: (T) -> Boolean,
        consumed: (ConsumedKStream<T>) -> Unit
    ): BranchedKStream<T> {
        val branchNamed = "branch-$nextBranchNumber"

        stream.branch(
            { _, value -> predicate(value) },
            Branched.withConsumer({
                consumed(
                    ConsumedKStream(
                        topic = topic,
                        stream = it,
                        namedSupplier = { "via-$branchNamed-$named" },
                    )
                )
            }, "-$branchNamed")
        )
        return this
    }

    fun default(consumed: (ConsumedKStream<T>) -> Unit) {
        val branchNamed = "branch-default"
        stream.defaultBranch(
            Branched.withConsumer({
                consumed(
                    ConsumedKStream(
                        topic = topic,
                        stream = it,
                        namedSupplier = { "via-$branchNamed-$named" },
                    )
                )
            }, "-$branchNamed")
        )
    }
}

class BranchedMappedKStream<T : Any> internal constructor(
    private val sourceTopicName: String,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, T>,
    private val named: String,
) {
    private var nextBranchNumber: Int = 1
        get() = field++

    fun branch(
        predicate: (T) -> Boolean,
        consumed: (MappedKStream<T>) -> Unit,
    ): BranchedMappedKStream<T> {
        val branchNamed = "branch-$nextBranchNumber"

        stream.branch(
            { _, value -> predicate(value) },
            Branched.withConsumer({
                consumed(
                    MappedKStream(
                        sourceTopicName = sourceTopicName,
                        stream = it,
                        namedSupplier = { "via-$branchNamed-$named" },
                    )
                )
            }, "-$branchNamed")
        )
        return this
    }

    fun default(consumed: (MappedKStream<T>) -> Unit) {
        val branchNamed = "branch-default"
        stream.defaultBranch(
            Branched.withConsumer({
                consumed(MappedKStream(
                    sourceTopicName = sourceTopicName,
                    stream = it,
                    namedSupplier = { "via-$branchNamed-$named" }
                ))
            }, "-$branchNamed")
        )
    }
}
