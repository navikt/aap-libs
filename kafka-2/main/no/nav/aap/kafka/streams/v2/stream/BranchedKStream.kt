package no.nav.aap.kafka.streams.v2.stream

import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.KStream

class BranchedKStream<T : Any> internal constructor(
    private val topic: Topic<T>,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, T>,
    private val namedSupplier: () -> String,
) {
    private var nextBranchNumber: Int = 1
        get() = field++

    fun branch(
        predicate: (T) -> Boolean,
        consumed: (ConsumedKStream<T>) -> Unit,
    ): BranchedKStream<T> {
        val namedBranch = "-branch-$nextBranchNumber"
        val internalPredicate = internalPredicate(predicate)
        val internalBranch = internalBranch(consumed, namedBranch) { "via$namedBranch-${namedSupplier()}" }
        stream.branch(internalPredicate, internalBranch)
        return this
    }

    fun default(consumed: (ConsumedKStream<T>) -> Unit) {
        val namedBranch = "-branch-default"
        val internalBranch = internalBranch(consumed, namedBranch) { "via$namedBranch-${namedSupplier()}" }
        stream.defaultBranch(internalBranch)
    }

    private fun internalBranch(
        branch: (ConsumedKStream<T>) -> Unit,
        namedBranch: String,
        namedSupplier: () -> String,
    ): Branched<String, T> = Branched.withConsumer(
        { chain: KStream<String, T> -> branch(ConsumedKStream(topic, chain, namedSupplier)) },
        namedBranch
    )
}

class BranchedMappedKStream<T : Any> internal constructor(
    private val sourceTopicName: String,
    private val stream: org.apache.kafka.streams.kstream.BranchedKStream<String, T>,
    private val namedSupplier: () -> String,
) {
    private var nextBranchNumber: Int = 1
        get() = field++

    fun branch(
        predicate: (T) -> Boolean,
        consumed: (MappedKStream<T>) -> Unit,
    ): BranchedMappedKStream<T> {
        val namedBranch = "-branch-$nextBranchNumber"
        val internalPredicate = internalPredicate(predicate)
        val internalBranch = internalBranch(consumed, namedBranch) { "via$namedBranch-${namedSupplier()}" }
        stream.branch(internalPredicate, internalBranch)
        return this
    }

    fun default(consumed: (MappedKStream<T>) -> Unit) {
        val namedBranch = "-branch-default"
        val internalBranch = internalBranch(consumed, namedBranch) { "via$namedBranch-${namedSupplier()}" }
        stream.defaultBranch(internalBranch)
    }

    private fun internalBranch(
        branch: (MappedKStream<T>) -> Unit,
        namedBranch: String,
        namedSupplier: () -> String,
    ): Branched<String, T> = Branched.withConsumer(
        { chain: KStream<String, T> -> branch(MappedKStream(sourceTopicName, chain, namedSupplier)) },
        namedBranch
    )
}

private fun <T> internalPredicate(predicate: (T) -> Boolean) = { _: String, value: T -> predicate(value) }
