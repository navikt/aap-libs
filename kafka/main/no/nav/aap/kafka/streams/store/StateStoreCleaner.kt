package no.nav.aap.kafka.streams.store

import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.slf4j.LoggerFactory
import java.time.Duration

internal class StateStoreCleaner<V>(
    private val storeName: String,
    private val predicate: (value: ValueAndTimestamp<V>) -> Boolean,
) : Processor<String, V, Void, Void> {
    override fun process(record: Record<String, V>) {}

    override fun init(context: ProcessorContext<Void, Void>) {
        val store = context.getStateStore<KeyValueStore<String, ValueAndTimestamp<V>>>(storeName)
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME) {
            store.all().use { iterator ->
                iterator.forEach { record ->
                    if (predicate(record.value)) {
                        store.delete(record.key)
                        secureLog.info("Deleted [$storeName] [${record.key}] [${record.value}]")
                    }
                }
            }
        }
    }

    private companion object {
        private val secureLog = LoggerFactory.getLogger("secureLog")
    }
}

fun <V> KTable<String, V>.scheduleCleanup(
    store: String,
    predicate: (value: ValueAndTimestamp<V>) -> Boolean
) = toStream().process(
    ProcessorSupplier { StateStoreCleaner(store, predicate) },
    store
)
