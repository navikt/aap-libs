package no.nav.aap.kafka

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import net.logstash.logback.marker.ObjectAppendingMarker

class SecureLogAppender : ListAppender<ILoggingEvent>() {

    fun contains(message: String, level: Level = Level.TRACE) =
        list.any { it.message.contains(message) && it.level.equals(level) }

    fun firstContaining(substring: String, level: Level = Level.TRACE): ILoggingEvent = list
        .first { it.message.contains(substring) && it.level == level }

}

fun ILoggingEvent.structuredArguments() = argumentArray
    .mapNotNull { it as? ObjectAppendingMarker }
    .map { it.toStringSelf() }
    .associate {
        val (key, value) = it.split("=")
        key to value
    }
