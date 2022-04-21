package no.nav.aap.kafka.streams

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Produced

data class Topic<V>(
    val name: String,
    val valueSerde: Serde<V>,
    val keySerde: Serde<String> = Serdes.StringSerde(),
) {
    internal fun consumed(named: String): Consumed<String, V> = Consumed.with(keySerde, valueSerde).withName(named)
    internal fun produced(named: String): Produced<String, V> = Produced.with(keySerde, valueSerde).withName(named)

}

infix fun <L, R : Any> Topic<L>.with(right: Topic<R>): Joined<String, L, R> =
    Joined.with(keySerde, valueSerde, right.valueSerde, "$name-joined-${right.name}")
