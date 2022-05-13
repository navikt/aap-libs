package no.nav.aap.kafka.streams

import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.BranchedKStream
import org.apache.kafka.streams.kstream.KStream

interface BehovVisitor<out JSON> {
    fun toJson(): JSON
}

interface Behov<out JSON, in BEHOV_VISITOR : BehovVisitor<JSON>> {
    fun accept(visitor: BEHOV_VISITOR)
}

fun <JSON : Any, BEHOV_VISITOR : BehovVisitor<JSON>, BEHOV : Behov<JSON, BEHOV_VISITOR>>
        KStream<String, BEHOV>.sendBehov(
    name: String,
    block: BranchedKStream<String, BEHOV>.() -> Unit
) {
    split(named("$name-split-behov")).apply(block)
}

fun <JSON : Any, BEHOV_VISITOR : BehovVisitor<JSON>, BEHOV : Behov<JSON, BEHOV_VISITOR>>
        BranchedKStream<String, BEHOV>.branch(
    topic: Topic<JSON>,
    branchName: String,
    predicate: (BEHOV) -> Boolean,
    getMapper: () -> BEHOV_VISITOR
): BranchedKStream<String, BEHOV> =
    branch(branchName, predicate) { chain ->
        chain
            .mapValues("branch-$branchName-map-behov") { value -> getMapper().also { value.accept(it) }.toJson() }
            .produce(topic, "branch-$branchName-produced-behov")
    }

private fun <BEHOV_VISITOR : BehovVisitor<*>, BEHOV : Behov<*, BEHOV_VISITOR>> 
        BranchedKStream<String, BEHOV>.branch(
    branchName: String,
    predicate: (BEHOV) -> Boolean,
    chain: (KStream<String, BEHOV>) -> Unit
): BranchedKStream<String, BEHOV> = branch(
    { _, value -> predicate(value) },
    Branched.withConsumer(chain).withName("-branch-$branchName")
)
