package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.Topic
import no.nav.aap.kafka.streams.extension.join
import no.nav.aap.kafka.streams.extension.produce
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.KStream

class ConsumedKStream<V : Any>(
    private val topic: Topic<V>,
    private val kstream: KStream<String, V>,
) {
    fun produce(table: Table<V>): KTable<V> = KTable(topic, kstream.produce(table))

    fun produce(destination: Topic<V>) {
        kstream.produce(destination, "produced-to${destination.name}-from-${topic.name}")
    }

    fun rekey(selectKeyFromValue: (V) -> String): ConsumedKStream<V> {
        val stream = kstream.selectKey { _, value -> selectKeyFromValue(value) }
        return ConsumedKStream(topic, stream)
    }

    fun filter(lambda: (V) -> Boolean): ConsumedKStream<V> {
        val stream = kstream.filter { _, value -> lambda(value) }
        return ConsumedKStream(topic, stream)
    }

    fun <R : Any> joinWith(table: KTable<R>): JoinedKStream<V, R> {
        val joinedStream = kstream.join(topic with table.topic, table.table, ::KStreamPair)
        return JoinedKStream(left = topic, right = table.topic, joinedStream)
    }

    fun <R : Any> leftJoinWith(table: KTable<R>): LeftJoinedKStream<V, R?> {
        val joinedStream = kstream.leftJoin(topic withNullable table.topic, table.table, ::KStreamPair)
        return LeftJoinedKStream(left = topic, right = table.topic, kstream = joinedStream)
    }
}

private fun <L, R, LR> KStream<String, L>.leftJoin(
    joined: Joined<String, L, R?>,
    table: org.apache.kafka.streams.kstream.KTable<String, R>,
    joiner: (L, R?) -> LR,
): KStream<String, LR> = join(table, joiner, joined)
