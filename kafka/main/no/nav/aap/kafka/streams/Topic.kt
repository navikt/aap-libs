package no.nav.aap.kafka.streams

import no.nav.aap.kafka.streams.concurrency.Bufferable
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Produced

open class Topic<V>(
    val name: String,
    val valueSerde: Serde<V>,
    val keySerde: Serde<String> = Serdes.StringSerde(),
) {
    internal fun consumed(named: String): Consumed<String, V> = Consumed.with(keySerde, valueSerde).withName(named)
    internal open fun produced(named: String): Produced<String, V> = Produced.with(keySerde, valueSerde).withName(named)

    infix fun <R : Any> with(right: Topic<R>): Joined<String, V, R> = Joined.with(
        keySerde,
        valueSerde,
        right.valueSerde,
        "$name-joined-${right.name}",
    )

    infix fun <R : Bufferable<R>> with(right: BufferableTopic<R>) = right.withBuffer(this)
}
