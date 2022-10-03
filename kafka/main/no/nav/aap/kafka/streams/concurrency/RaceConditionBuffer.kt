package no.nav.aap.kafka.streams.concurrency

import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

interface Bufferable<V> {
    fun erNyere(other: V): Boolean
}

class RaceConditionBuffer<K, V : Bufferable<V>>(
    private val levetidSekunder: Long = 10,
    private val logRecordValues: Boolean = false,
    private val logDebug: Boolean = false,
) {
    private val buffer = ConcurrentHashMap<K, BufferElement<V>>()
    private val logger = LoggerFactory.getLogger("secureLog")

    fun lagre(key: K, value: V) {
        buffer[key] = BufferElement(Instant.now(), value)

        if (logDebug) {
            if (logRecordValues) logger.debug("Lagrer buffer for $key=$value")
            else logger.debug("Lagrer buffer for $key")
        }

        slettGamle()
    }

    fun velgNyeste(key: K, other: V): V {
        slettGamle()

        return buffer[key]
            ?.value
            ?.takeIf { it.erNyere(other) }
            ?.also {
                if (logRecordValues) logger.info("Bruker buffer for $key=$it")
                else logger.info("Bruker buffer for $key")
            } ?: other
    }

    private fun slettGamle() {
        buffer.keys.forEach { key ->
            buffer.computeIfPresent(key) { _, v ->
                v.takeIf { it.timestamp.plusSeconds(levetidSekunder) > Instant.now() }
            }
        }
    }

    private class BufferElement<V : Bufferable<V>>(
        val timestamp: Instant,
        val value: V,
    )
}
