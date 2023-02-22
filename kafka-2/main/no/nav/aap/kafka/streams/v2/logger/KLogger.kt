package no.nav.aap.kafka.streams.v2.logger

import org.slf4j.LoggerFactory

class Log(name: String) {
    private val logger = LoggerFactory.getLogger(name)

    fun trace(msg: String) = logger.trace(msg)
    fun debug(msg: String) = logger.debug(msg)
    fun info(msg: String) = logger.info(msg)
    fun warn(msg: String) = logger.warn(msg)
    fun error(msg: String) = logger.error(msg)

    companion object {
        val secure: Log by lazy { Log("secureLog") }
    }
}
