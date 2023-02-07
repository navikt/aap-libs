package no.nav.aap.kafka.streams.v2.logger

import org.slf4j.LoggerFactory

internal object KLogger {
    internal fun log(lvl: LogLevel, msg: String) = when (lvl) {
        LogLevel.TRACE -> logger.trace(msg)
        LogLevel.DEBUG -> logger.debug(msg)
        LogLevel.INFO -> logger.info(msg)
        LogLevel.WARN -> logger.warn(msg)
        LogLevel.ERROR -> logger.error(msg)
    }

}

private val logger by lazy { LoggerFactory.getLogger("secureLog") }

enum class LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR
}
