package libs.kafka

import libs.kafka.processor.state.StateInitProcessor
import libs.kafka.processor.state.StateScheduleProcessor
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
