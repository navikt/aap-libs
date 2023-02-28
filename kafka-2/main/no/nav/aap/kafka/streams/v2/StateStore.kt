package no.nav.aap.kafka.streams.v2

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ValueAndTimestamp

class StateStore<T>(private val internalStateStore: ReadOnlyKeyValueStore<String, ValueAndTimestamp<T>>) {
    infix operator fun get(key: String): T? = internalStateStore[key]?.value()

    fun forEach(loop: (key: String, value: T) -> Unit) =
        internalStateStore.all().use { iterator ->
            iterator.asSequence().forEach { record ->
                loop(record.key, record.value.value())
            }
        }
}
