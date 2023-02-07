package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.v2.processor.state.StateScheduleProcessor

class KTable<T>(
    val table: Table<T>,
    val internalTable: org.apache.kafka.streams.kstream.KTable<String, T>,
) {

    fun schedule(scheduler: StateScheduleProcessor<T>) = scheduler.addToStreams()
}
