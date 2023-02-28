package no.nav.aap.kafka.streams.v2.processor.state

import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.StateStore
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import kotlin.time.Duration
import kotlin.time.toJavaDuration

internal interface KStateScheduleProcessor<T> {
    fun schedule(timestamp: Long, store: StateStore<T>)
}

abstract class StateScheduleProcessor<T>(
    private val named: String,
    private val table: KTable<T>,
    private val interval: Duration,
) : KStateScheduleProcessor<T> {
    internal fun addToStreams() {
        val stateStoreName = table.table.stateStoreName
        val internalStream = table.internalKTable.toStream()
        internalStream.processValues(
            { InternalProcessor(stateStoreName) },
            Named.`as`(named),
            stateStoreName,
        )
    }

    private inner class InternalProcessor(private val stateStoreName: String) : FixedKeyProcessor<String, T, T> {
        override fun init(context: FixedKeyProcessorContext<String, T>) {
            val store: TimestampedKeyValueStore<String, T> = context.getStateStore(stateStoreName)
            context.schedule(interval.toJavaDuration(), PunctuationType.WALL_CLOCK_TIME) { timestamp ->
                schedule(timestamp, StateStore(store))
            }
        }

        override fun process(record: FixedKeyRecord<String, T>) {}
    }
}
