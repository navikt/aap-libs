package no.nav.aap.kafka.streams.v2.extension

import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.Table
import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named

fun <L, R, LR> KStream<String, L>.leftJoin(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R?) -> LR,
): KStream<String, LR> =
    leftJoin(
        right.internalTable,
        joiner,
        left leftJoin right
    )

fun <L, R, LR> KStream<String, L>.join(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R) -> LR,
): KStream<String, LR> =
    join(
        right.internalTable,
        joiner,
        left join right
    )

@Suppress("UNCHECKED_CAST")
internal fun <K, V> KStream<K, V>.filterNotNull(
): KStream<K, V & Any> = filter { _, value -> value != null } as KStream<K, V & Any>

@Suppress("UNCHECKED_CAST")
internal fun <K, V> org.apache.kafka.streams.kstream.KTable<K, V>.skipTombstone(
    table: Table<V>,
): org.apache.kafka.streams.kstream.KTable<K, V & Any> = filter(
    { _, value -> value != null },
    Named.`as`("skip-table-${table.name}-tombstone")
) as org.apache.kafka.streams.kstream.KTable<K, V & Any>

@Suppress("UNCHECKED_CAST")
internal fun <K, V> KStream<K, V?>.skipTombstone(topic: Topic<V>): KStream<K, V & Any> = filter(
    { _, value -> value != null },
    Named.`as`("skip-${topic.name}-tombstone")
) as KStream<K, V & Any>
