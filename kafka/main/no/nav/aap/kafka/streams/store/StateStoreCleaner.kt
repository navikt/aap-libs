package no.nav.aap.kafka.streams.store

import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.slf4j.LoggerFactory
import java.time.Duration

private class StateStoreCleaner<K, V>(
    private val storeName: String,
    private val keysToDelete: MutableList<K>,
) : Processor<K, V, Void, Void> {
    override fun process(record: Record<K, V>) {}

    override fun init(context: ProcessorContext<Void, Void>) {
        val store = context.getStateStore<KeyValueStore<K, ValueAndTimestamp<V>>>(storeName)
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME) {
            keysToDelete.removeIf { key ->
                val value = store.delete(key)
                secureLog.info("Deleted [$storeName] [$key] [$value]")
                true
            }
        }
    }

    private companion object {
        private val secureLog = LoggerFactory.getLogger("secureLog")
    }
}

fun <K, V> KTable<K, V>.scheduleCleanup(
    store: String,
    keysToDelete: MutableList<K>,
) = scheduleCleanup(store, keysToDelete, "Cleanup $store")

fun <K, V> KTable<K, V>.scheduleCleanup(
    store: String,
    keysToDelete: MutableList<K>,
    name: String,
) = toStream().process(
    ProcessorSupplier { StateStoreCleaner(store, keysToDelete) },
    Named.`as`(name),
    store
)
