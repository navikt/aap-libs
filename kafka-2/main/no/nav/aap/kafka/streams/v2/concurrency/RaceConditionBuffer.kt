package no.nav.aap.kafka.streams.v2.concurrency

import no.nav.aap.cache.Cache
import no.nav.aap.kafka.streams.concurrency.Bufferable
import org.slf4j.LoggerFactory
import java.time.temporal.ChronoUnit

class RaceConditionBuffer<V : Bufferable<V>>(
    levetidSekunder: Long = 10,
    private val logRecordValues: Boolean = false,
    private val logDebug: Boolean = false,
) {
    private val buffer = Cache<String, V>(levetidSekunder, ChronoUnit.SECONDS)
    private val logger = LoggerFactory.getLogger("secureLog")

    internal fun lagre(key: String, value: V) {
        buffer[key] = value

        if (logDebug) {
            if (logRecordValues) logger.debug("Lagrer buffer for $key=$value")
            else logger.debug("Lagrer buffer for $key")
        }
    }

    internal fun slett(key: String) {
        buffer.remove(key)

        if (logDebug) {
            logger.debug("Sletter buffer for $key")
        }
    }

    internal fun velgNyeste(key: String, other: V): V =
        buffer[key]
            ?.takeIf { it.erNyere(other) }
            ?.also {
                if (logRecordValues) logger.info("Bruker buffer for $key=$it")
                else logger.info("Bruker buffer for $key")
            } ?: other

    internal fun velgNyesteNullable(key: String, other: V?): V? =
        buffer[key]
            ?.takeIf { other != null && it.erNyere(other) }
            ?.also {
                if (logRecordValues) logger.info("Bruker buffer for $key=$it")
                else logger.info("Bruker buffer for $key")
            } ?: other
}
