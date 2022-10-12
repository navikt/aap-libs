package no.nav.aap.kafka.streams.concurrency

interface Bufferable<V> {
    fun erNyere(other: V): Boolean
}
