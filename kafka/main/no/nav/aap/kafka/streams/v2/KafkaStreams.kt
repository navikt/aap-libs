package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.extension.filterNotNull
import no.nav.aap.kafka.streams.materialized
import no.nav.aap.kafka.streams.named
import no.nav.aap.kafka.streams.transformer.LogProduceTable
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier


fun <V> KStream<String, V>.produce(table: Table<V>): org.apache.kafka.streams.kstream.KTable<String, V & Any> =
    this
        .logProduced(table, named("log-produced-${table.name}"))
        .filterNotNull("filter-not-null-${table.name}-as-table")// todo: skal denne før eller etter toTable?
        .toTable(named("${table.name}-as-table"), materialized(table.stateStoreName, table.source))


internal fun <K, V> KStream<K, V>.logProduced(
    table: Table<V>,
    named: Named,
): KStream<K, V?> = processValues(
    FixedKeyProcessorSupplier { LogProduceTable<K, V>("Produserer til KTable", table) },
    named
)
