package no.nav.aap.kafka.streams

import no.nav.aap.kafka.streams.concurrency.Bufferable
import no.nav.aap.kafka.streams.concurrency.BufferableJoined
import no.nav.aap.kafka.streams.concurrency.RaceConditionBuffer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Produced

class BufferableTopic<V : Bufferable<V>>(
    name: String,
    valueSerde: Serde<V>,
    private val buffer: RaceConditionBuffer<String, V>,
    keySerde: Serde<String> = Serdes.StringSerde(),
) : Topic<V>(
    name = name,
    valueSerde = valueSerde,
    keySerde = keySerde
) {

    override fun produced(named: String): Produced<String, V> =
        Produced.with(keySerde, valueSerde).withName("$named-buffered")

    internal infix fun <L> withBuffer(left: Topic<L>): BufferableJoined<String, L, V> = BufferableJoined<String, L, V>(
        buffer, Joined.with(
            keySerde,
            left.valueSerde,
            valueSerde,
            "$name-buffered-joined-${left.name}",
        )
    )

    internal fun lagreBuffer(key: String, value: V) = buffer.lagre(key, value)
}
