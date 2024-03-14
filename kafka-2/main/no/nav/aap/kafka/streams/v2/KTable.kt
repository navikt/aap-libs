package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.v2.extension.skipTombstone
import no.nav.aap.kafka.streams.v2.processor.state.StateInitProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateScheduleProcessor
import org.apache.kafka.streams.kstream.KTable

class KTable<T : Any>(
    val table: Table<T>,
    val internalKTable: KTable<String, T?>,
) {
    internal val tombstonedInternalKTable: KTable<String, T> by lazy {
        internalKTable.skipTombstone(table)
    }

    fun schedule(scheduler: StateScheduleProcessor<T>) {
        scheduler.addToStreams()
    }

    fun init(processor: StateInitProcessor<T>) {
        processor.addToStreams()
    }
}
