package no.nav.aap.kafka.streams.concurrency

interface Bufferable {
    fun erNyere(other: Bufferable): Boolean
}

class RaceConditionBuffer<K, V: Bufferable> {

    private val buffer = mutableMapOf<K, V>()

    fun lagre(key: K, value: V) {
        buffer[key] = value
    }

    fun velgNyeste(key: K, other: V): V =
        buffer[key]?.takeIf { it.erNyere(other) } ?: other
}
