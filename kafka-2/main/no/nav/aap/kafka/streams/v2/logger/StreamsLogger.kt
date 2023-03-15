package no.nav.aap.kafka.streams.v2.logger

import net.logstash.logback.argument.StructuredArgument
import org.slf4j.LoggerFactory

class Log(name: String) {
    private val logger = LoggerFactory.getLogger(name)

    fun trace(msg: String, vararg labels: StructuredArgument) = logger.trace(msg, *labels)
    fun debug(msg: String, vararg labels: StructuredArgument) = logger.debug(msg, *labels)
    fun info(msg: String, vararg labels: StructuredArgument) = logger.info(msg, *labels)
    fun warn(msg: String, vararg labels: StructuredArgument) = logger.warn(msg, *labels)
    fun error(msg: String, vararg labels: StructuredArgument) = logger.error(msg, *labels)

    companion object {
        val secure: Log by lazy { Log("secureLog") }
    }
}
