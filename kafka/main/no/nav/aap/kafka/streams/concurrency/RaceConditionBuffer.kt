package no.nav.aap.kafka.streams.concurrency

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

interface Bufferable<V> {
    fun erNyere(other: V): Boolean
}

class RaceConditionBuffer<K, V: Bufferable<V>> {
    private val buffer = ConcurrentHashMap<K, BufferElement<V>>()

    fun lagre(key: K, value: V) {
        buffer[key] = BufferElement(Instant.now(), value)
        slettGamle()
    }

    fun velgNyeste(key: K, other: V): V {
        slettGamle()

        return buffer[key]
            ?.value
            ?.takeIf { it.erNyere(other) }
            ?: other
    }

    private fun slettGamle() {
        buffer.keys.forEach { key ->
            buffer.computeIfPresent(key) { _, v ->
                v.takeIf { it.timestamp.plusSeconds(10) > Instant.now() }
            }
        }
    }

    private class BufferElement<V: Bufferable<V>>(
        val timestamp: Instant,
        val value: V,
    )
}
