package no.nav.aap.kafka.streams.v2

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

    internal infix fun <R> join(right: KTable<R>): Joined<String, V, R> = Joined.with(
        keySerde,
        valueSerde,
        right.topic.valueSerde,
        "$name-join-${right.topic.name}",
    )

    internal infix fun <R> leftJoin(right: KTable<R>): Joined<String, V, R> = Joined.with(
        keySerde,
        valueSerde,
        right.topic.valueSerde,
        "$name-left-join-${right.topic.name}",
    )
}
