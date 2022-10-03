package no.nav.aap.kafka.streams.concurrency

import org.apache.kafka.streams.kstream.Joined

class BufferableJoined<K, L, R: Bufferable<R>>(
    private val buffer: RaceConditionBuffer<K, R>,
    val joined: Joined<K, L, R>,
) {
    internal fun velgNyeste(key: K, other: R): R = buffer.velgNyeste(key, other)
}
