package no.nav.aap.kafka.streams.v2.extension

import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.NullableKStreamPair
import no.nav.aap.kafka.streams.v2.Table
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.logger.KLogger
import no.nav.aap.kafka.streams.v2.logger.LogLevel
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named

internal fun <L, R, LR> KStream<String, L>.leftJoin(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R?) -> LR,
): KStream<String, LR> = this
    .leftJoin(
        right.internalTable,
        joiner,
        left leftJoin right
    )

internal fun <L, R, LR> KStream<String, L>.join(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R) -> LR,
): KStream<String, LR> = this
    .join(
        right.internalTable,
        joiner,
        left join right
    )

internal fun <K, V> KStream<K, V>.log(
    lvl: LogLevel,
    keyValue: (K, V) -> Any,
): KStream<K, V> = this
    .peek { key, value ->
        KLogger.log(lvl, keyValue(key, value).toString())
    }

internal fun <K, L, R> KStream<K, NullableKStreamPair<L, R>>.log(
    lvl: LogLevel,
    keyValue: (K, L, R?) -> Any,
): KStream<K, NullableKStreamPair<L, R>> = this
    .peek { key, (left, right) ->
        KLogger.log(lvl, keyValue(key, left, right).toString())
    }

@Suppress("UNCHECKED_CAST")
internal fun <K, V> KStream<K, V>.filterNotNull(): KStream<K, V & Any> = this
    .filter { _, value -> value != null } as KStream<K, V & Any>

@Suppress("UNCHECKED_CAST")
internal fun <K, V> org.apache.kafka.streams.kstream.KTable<K, V>.skipTombstone(
    table: Table<V>,
): org.apache.kafka.streams.kstream.KTable<K, V & Any> = this
    .filter(
        { _, value -> value != null },
        Named.`as`("skip-table-${table.sourceTopicName}-tombstone")
    ) as org.apache.kafka.streams.kstream.KTable<K, V & Any>

@Suppress("UNCHECKED_CAST")
internal fun <K, V> KStream<K, V?>.skipTombstone(topic: Topic<V>): KStream<K, V & Any> = this
    .filter(
        { _, value -> value != null },
        Named.`as`("skip-${topic.name}-tombstone")
    ) as KStream<K, V & Any>
