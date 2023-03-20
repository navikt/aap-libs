package no.nav.aap.kafka.streams.v2.extension

import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.Table
import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.processor.LogProduceTopicProcessor
import no.nav.aap.kafka.streams.v2.processor.Processor.Companion.addProcessor
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named

internal fun <T : Any> KStream<String, T>.produceWithLogging(
    topic: Topic<T>,
    named: String,
) = this
    .addProcessor(LogProduceTopicProcessor("log-${named}", topic))
    .to(topic.name, topic.produced(named))

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

internal fun <L : Any, R : Any, LR> KStream<String, L>.leftJoin(
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
        right.tombstonedInternalKTable,
        joiner,
        left join right
    )

internal fun <L : Any, R : Any, LR> KStream<String, L>.join(
    left: Topic<L>,
    right: KTable<R>,
    joiner: (L, R) -> LR,
): KStream<String, LR> = this
    .join(
        right.tombstonedInternalKTable,
        joiner,
        left join right
    )

@Suppress("UNCHECKED_CAST")
internal fun <V> KStream<String, V>.filterNotNull(): KStream<String, V & Any> = this
    .filter { _, value -> value != null } as KStream<String, V & Any>

@Suppress("UNCHECKED_CAST")
internal fun <V : Any> org.apache.kafka.streams.kstream.KTable<String, V?>.skipTombstone(
    table: Table<V>,
): org.apache.kafka.streams.kstream.KTable<String, V> = this
    .filter(
        { _, value -> value != null },
        Named.`as`("skip-table-${table.sourceTopicName}-tombstone")
    ) as org.apache.kafka.streams.kstream.KTable<String, V>

@Suppress("UNCHECKED_CAST")
internal fun <V : Any> KStream<String, V?>.skipTombstone(topic: Topic<V>): KStream<String, V> = this
    .filter(
        { _, value -> value != null },
        Named.`as`("skip-${topic.name}-tombstone")
    ) as KStream<String, V>
