package no.nav.aap.kafka.streams.extension

import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.KStream

/**
 * @param logValue Logs record values to secure-logs when true
 */
fun <V> StreamsBuilder.consume(topic: Topic<V>, logValue: Boolean = false): KStream<String, V> = this
    .stream(topic.name, topic.consumed("consume-${topic.name}"))
    .logConsumed(topic, logValue)

fun <V> StreamsBuilder.globalTable(table: Table<V>): GlobalKTable<String, V> =
    globalTable(table.source.name, table.source.consumed("${table.name}-as-globaltable"))
