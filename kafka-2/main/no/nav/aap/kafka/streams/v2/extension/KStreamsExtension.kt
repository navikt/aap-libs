package no.nav.aap.kafka.streams.v2.extension

import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.Table
import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named

internal fun <L : Any, R : Any, LR> KStream<String, L>.leftJoin(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (String, L, R?) -> LR,
): KStream<String, LR> = this
    .leftJoin(
        right.internalKTable,
        joiner,
        left leftJoin right
    )

internal fun <L: Any, R: Any, LR> KStream<String, L>.leftJoin(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R?) -> LR,
): KStream<String, LR> = this
    .leftJoin(
        right.internalKTable,
        joiner,
        left leftJoin right
    )

internal fun <L : Any, R : Any, LR> KStream<String, L>.join(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (String, L, R) -> LR,
): KStream<String, LR> = this
    .join(
        right.internalKTable,
        joiner,
        left join right
    )

internal fun <L: Any, R: Any, LR> KStream<String, L>.join(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R) -> LR,
): KStream<String, LR> = this
    .join(
        right.internalKTable,
        joiner,
        left join right
    )

@Suppress("UNCHECKED_CAST")
internal fun <K, V> KStream<K, V>.filterNotNull(): KStream<K, V & Any> = this
    .filter { _, value -> value != null } as KStream<K, V & Any>

@Suppress("UNCHECKED_CAST")
internal fun <K, V : Any> org.apache.kafka.streams.kstream.KTable<K, V?>.skipTombstone(
    table: Table<V>,
): org.apache.kafka.streams.kstream.KTable<K, V> = this
    .filter(
        { _, value -> value != null },
        Named.`as`("skip-table-${table.sourceTopicName}-tombstone")
    ) as org.apache.kafka.streams.kstream.KTable<K, V>

@Suppress("UNCHECKED_CAST")
internal fun <K, V : Any> KStream<K, V?>.skipTombstone(topic: Topic<V>): KStream<K, V> = this
    .filter(
        { _, value -> value != null },
        Named.`as`("skip-${topic.name}-tombstone")
    ) as KStream<K, V>
