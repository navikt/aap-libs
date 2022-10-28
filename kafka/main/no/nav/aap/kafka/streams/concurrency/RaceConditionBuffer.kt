package no.nav.aap.kafka.streams.concurrency

import no.nav.aap.cache.Cache
import org.slf4j.LoggerFactory
import java.time.temporal.ChronoUnit

class RaceConditionBuffer<in K, V : Bufferable<V>>(
    levetidSekunder: Long = 10,
    private val logRecordValues: Boolean = false,
    private val logDebug: Boolean = false,
) {
    private val buffer = Cache<K, V>(levetidSekunder, ChronoUnit.SECONDS)
    private val logger = LoggerFactory.getLogger("secureLog")

    internal fun lagre(key: K, value: V) {
        buffer[key] = value

        if (logDebug) {
            if (logRecordValues) logger.debug("Lagrer buffer for $key=$value")
            else logger.debug("Lagrer buffer for $key")
        }
    }

    internal fun velgNyeste(key: K, other: V): V =
        buffer[key]
            ?.takeIf { it.erNyere(other) }
            ?.also {
                if (logRecordValues) logger.info("Bruker buffer for $key=$it")
                else logger.info("Bruker buffer for $key")
            } ?: other
}
