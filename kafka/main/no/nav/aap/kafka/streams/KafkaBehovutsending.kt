package no.nav.aap.kafka.streams

import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.BranchedKStream
import org.apache.kafka.streams.kstream.KStream

interface BehovExtractor<out JSON> {
    fun toJson(): JSON
}

interface Behov<in BEHOV_VISITOR> {
    fun accept(visitor: BEHOV_VISITOR)
}

fun <BEHOV_VISITOR, BEHOV : Behov<BEHOV_VISITOR>>
        KStream<String, BEHOV>.sendBehov(
    name: String,
    block: BranchedKStream<String, BEHOV>.() -> Unit
) {
    split(named("$name-split-behov")).apply(block)
}

fun <JSON : Any, EXTRACTOR : BehovExtractor<JSON>, BEHOV : Behov<EXTRACTOR>>
        BranchedKStream<String, BEHOV>.branch(
    topic: Topic<JSON>,
    branchName: String,
    predicate: (BEHOV) -> Boolean,
    extractor: () -> EXTRACTOR
): BranchedKStream<String, BEHOV> =
    branch(branchName, predicate) { chain ->
        chain
            .mapValues("branch-$branchName-map-behov") { value -> extractor().also(value::accept).toJson() }
            .produce(topic, "branch-$branchName-produced-behov")
    }

private fun <BEHOV : Behov<*>>
        BranchedKStream<String, BEHOV>.branch(
    branchName: String,
    predicate: (BEHOV) -> Boolean,
    chain: (KStream<String, BEHOV>) -> Unit
): BranchedKStream<String, BEHOV> = branch(
    { _, value -> predicate(value) },
    Branched.withConsumer(chain).withName("-branch-$branchName")
)
