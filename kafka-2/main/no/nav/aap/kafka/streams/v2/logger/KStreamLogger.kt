package no.nav.aap.kafka.streams.v2.logger

import no.nav.aap.kafka.streams.v2.NullableKStreamPair
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory

internal fun <K, V> KStream<K, V>.log(
    lvl: LogLevel,
    keyValue: (K, V) -> Any,
): KStream<K, V> = peek { key, value ->
    log(lvl, keyValue(key, value).toString())
}

internal fun <K, L, R> KStream<K, NullableKStreamPair<L, R>>.log(
    lvl: LogLevel,
    keyValue: (K, L, R?) -> Any,
): KStream<K, NullableKStreamPair<L, R>> = peek { key, (left, right) ->
    log(lvl, keyValue(key, left, right).toString())
}

private fun log(lvl: LogLevel, msg: String) = when (lvl) {
    LogLevel.TRACE -> logger.trace(msg)
    LogLevel.DEBUG -> logger.debug(msg)
    LogLevel.INFO -> logger.info(msg)
    LogLevel.WARN -> logger.warn(msg)
    LogLevel.ERROR -> logger.error(msg)
}

enum class LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR
}

private val logger by lazy { LoggerFactory.getLogger("secureLog") }
