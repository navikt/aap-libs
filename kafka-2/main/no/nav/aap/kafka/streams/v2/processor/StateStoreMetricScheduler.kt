package no.nav.aap.kafka.streams.v2.processor

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import no.nav.aap.kafka.streams.v2.KTable
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

class StateStoreMetricScheduler<T>(
    named: String,
    table: KTable<T>,
    interval: Duration,
    registry: MeterRegistry
) : KStoreProcessorScheduler<T>(
    named = named,
    table = table,
    interval = interval,
) {
    private val approximateNumberOfRecords = AtomicLong()

    init {
        registry.gauge(
            "kafka_stream_state_store_entries",
            listOf(Tag.of("store", table.table.stateStoreName)),
            approximateNumberOfRecords
        )
    }

    override fun schedule(timestamp: Long, store: TimestampedKeyValueStore<String, T>) {
        approximateNumberOfRecords.set(store.approximateNumEntries())
    }
}
