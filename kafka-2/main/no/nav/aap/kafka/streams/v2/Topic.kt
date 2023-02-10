package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.v2.serde.StreamSerde
import no.nav.aap.kafka.streams.v2.serde.StringSerde
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Produced

open class Topic<T : Any>(
    val name: String,
    val valueSerde: StreamSerde<T>,
    val keySerde: StreamSerde<String> = StringSerde,
) {
    internal fun consumed(named: String): Consumed<String, T> = Consumed.with(keySerde, valueSerde).withName(named)
    internal open fun produced(named: String): Produced<String, T> = Produced.with(keySerde, valueSerde).withName(named)

    internal infix fun <U : Any> join(right: KTable<U>): Joined<String, T, U> = Joined.with(
        keySerde,
        valueSerde,
        right.table.sourceTopic.valueSerde,
        "$name-join-${right.table.sourceTopic.name}",
    )

    internal infix fun <U : Any> leftJoin(right: KTable<U>): Joined<String, T, U?> = Joined.with(
        keySerde,
        valueSerde,
        right.table.sourceTopic.valueSerde,
        "$name-left-join-${right.table.sourceTopic.name}",
    )
}
