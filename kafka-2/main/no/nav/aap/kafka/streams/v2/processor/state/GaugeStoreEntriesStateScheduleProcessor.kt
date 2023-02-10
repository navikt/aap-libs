package no.nav.aap.kafka.streams.v2.processor.state

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import no.nav.aap.kafka.streams.v2.KTable
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

class GaugeStoreEntriesStateScheduleProcessor<T:Any>(
    ktable: KTable<T>,
    interval: Duration,
    registry: MeterRegistry
) : StateScheduleProcessor<T>(
    named = "gauge-${ktable.table.stateStoreName}-entries",
    table = ktable,
    interval = interval,
) {
    private val approximateNumberOfRecords = AtomicLong()

    init {
        registry.gauge(
            "kafka_stream_state_store_entries",
            listOf(Tag.of("store", ktable.table.stateStoreName)),
            approximateNumberOfRecords
        )
    }

    override fun schedule(timestamp: Long, store: TimestampedKeyValueStore<String, T>) {
        approximateNumberOfRecords.set(store.approximateNumEntries())
    }
}
