package no.nav.aap.kafka.streams.v2

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Produced

open class Topic<T>(
    val name: String,
    val valueSerde: Serde<T>,
    val keySerde: Serde<String> = Serdes.StringSerde(),
) {
    internal fun consumed(named: String): Consumed<String, T> = Consumed.with(keySerde, valueSerde).withName(named)
    internal open fun produced(named: String): Produced<String, T> = Produced.with(keySerde, valueSerde).withName(named)

    internal infix fun <U> join(right: KTable<U>): Joined<String, T, U> = Joined.with(
        keySerde,
        valueSerde,
        right.table.source.valueSerde,
        "$name-join-${right.table.source.name}",
    )

    internal infix fun <U> leftJoin(right: KTable<U>): Joined<String, T, U> = Joined.with(
        keySerde,
        valueSerde,
        right.table.source.valueSerde,
        "$name-left-join-${right.table.source.name}",
    )
}
