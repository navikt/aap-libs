package no.nav.aap.kafka.streams.store

import io.micrometer.core.instrument.MeterRegistry
import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.named
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.toJavaDuration

private class StateStoreMetrics<K, V>(
    private val table: Table<V>,
    private val interval: Duration,
    private val registry: MeterRegistry,
) : Processor<K, V, Void, Void> {
    private val numOfRec = AtomicLong()

    override fun process(record: Record<K, V>) {}

    override fun init(context: ProcessorContext<Void, Void>) {
        registry.gauge("kafka_stream_state_store_entries", numOfRec)
        val store = context.getStateStore<KeyValueStore<K, ValueAndTimestamp<V>>>(table.stateStoreName)
        context.schedule(interval.toJavaDuration(), PunctuationType.WALL_CLOCK_TIME) {
            numOfRec.set(store.approximateNumEntries())
        }
    }
}

fun <K, V> KTable<K, V>.scheduleMetrics(
    table: Table<V>,
    interval: Duration,
    registry: MeterRegistry,
) = toStream().process(
    ProcessorSupplier { StateStoreMetrics(table, interval, registry) },
    named("metrics-${table.stateStoreName}"),
    table.stateStoreName
)
