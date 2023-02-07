package no.nav.aap.kafka.streams.v2.processor.state

import no.nav.aap.kafka.streams.v2.KTable
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import kotlin.time.Duration
import kotlin.time.toJavaDuration

internal interface KStateScheduleProcessor<T> {
    fun schedule(timestamp: Long, store: TimestampedKeyValueStore<String, T>)
}

abstract class StateScheduleProcessor<T>(
    private val named: String,
    private val table: KTable<T>,
    private val interval: Duration,
) : KStateScheduleProcessor<T> {
    internal companion object {
        internal fun <T> initInternalProcessor(scheduler: StateScheduleProcessor<T>): KStream<String, T> {
            val stateStoreName = scheduler.table.table.stateStoreName
            val internalStream = scheduler.table.internalTable.toStream()
            return internalStream.processValues(
                { scheduler.run { InternalProcessor(stateStoreName) } },
                Named.`as`(scheduler.named),
                stateStoreName,
            )
        }
    }

    private inner class InternalProcessor(private val stateStoreName: String) : FixedKeyProcessor<String, T, T> {
        override fun init(context: FixedKeyProcessorContext<String, T>) {
            val store: TimestampedKeyValueStore<String, T> = context.getStateStore(stateStoreName)
            context.schedule(interval.toJavaDuration(), PunctuationType.WALL_CLOCK_TIME) { timestamp ->
                schedule(timestamp, store)
            }
        }

        override fun process(record: FixedKeyRecord<String, T>) {}
    }
}
