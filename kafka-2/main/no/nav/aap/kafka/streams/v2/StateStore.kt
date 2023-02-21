package no.nav.aap.kafka.streams.v2

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

class StateStore<T>(private val internalStateStore: ReadOnlyKeyValueStore<String, T>) {
    infix operator fun get(key: String): T? = internalStateStore[key]

    fun forEach(loop: (key: String, value: T) -> Unit) =
        internalStateStore.all().use { iterator ->
            iterator.asSequence().forEach { record ->
                loop(record.key, record.value)
            }
        }
}
