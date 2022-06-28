package no.nav.aap.kafka.streams.extension

import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.Topic
import no.nav.aap.kafka.streams.named
import no.nav.aap.kafka.streams.transformer.TraceLogTransformer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier

fun <V> StreamsBuilder.consume(topic: Topic<V>, logValue: Boolean = false): KStream<String, V?> =
    stream(topic.name, topic.consumed("consume-${topic.name}"))
        .transformValues(
            ValueTransformerWithKeySupplier {
                TraceLogTransformer<String, V>(
                    message = "Konsumerer Topic",
                    logValue = logValue,
                )
            }, named("log-consume-${topic.name}")
        )

fun <V> StreamsBuilder.globalTable(table: Table<V>): GlobalKTable<String, V> =
    globalTable(table.source.name, table.source.consumed("${table.name}-as-globaltable"))
