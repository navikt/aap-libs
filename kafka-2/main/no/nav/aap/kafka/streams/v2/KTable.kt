package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.v2.processor.state.StateInitProcessor
import no.nav.aap.kafka.streams.v2.processor.state.StateScheduleProcessor

class KTable<T>(
    val table: Table<T>,
    val internalKTable: org.apache.kafka.streams.kstream.KTable<String, T>,
) {

    fun schedule(scheduler: StateScheduleProcessor<T>) = scheduler.addToStreams()
    fun init(processor: StateInitProcessor<T>) = processor.addToStreams()
}
