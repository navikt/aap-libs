package no.nav.aap.kafka.streams.store

import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.named
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.toJavaDuration


typealias PurgeListener<K> = (K) -> Unit

private class StateStorePurger<K, V>(
    private val table: Table<V>,
    private val interval: Duration,
    private val purgeFlag: AtomicBoolean,
    private val purgeListeners: MutableList<PurgeListener<K>>,
) : Processor<K, V, Void, Void> {
    override fun process(record: Record<K, V>) {}

    override fun init(context: ProcessorContext<Void, Void>) {
        val store = context.getStateStore<KeyValueStore<K, ValueAndTimestamp<V>>>(table.stateStoreName)
        context.schedule(interval.toJavaDuration(), PunctuationType.WALL_CLOCK_TIME) {
            if (purgeFlag.getAndSet(false)) {
                secureLog.info("Sletter ca ${store.approximateNumEntries()} fra state store")
                store.all().forEach { keyValue ->
                    purgeListeners.forEach { listener -> listener(keyValue.key) }
                    store.delete(keyValue.key)
                }
                purgeListeners.clear()
            }
        }
    }

    private companion object {
        private val secureLog = LoggerFactory.getLogger("secureLog")
    }
}

fun <K, V> KTable<K, V>.purge(
    table: Table<V>,
    interval: Duration,
    purgeFlag: AtomicBoolean,
    purgeListeners: MutableList<PurgeListener<K>>,
) = toStream().process(
    ProcessorSupplier { StateStorePurger(table, interval, purgeFlag, purgeListeners) },
    named("purge-${table.stateStoreName}"),
    table.stateStoreName
)