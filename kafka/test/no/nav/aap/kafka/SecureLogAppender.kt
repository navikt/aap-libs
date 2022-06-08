package no.nav.aap.kafka

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender

class SecureLogAppender : ListAppender<ILoggingEvent>() {

    fun contains(message: String, level: Level = Level.INFO) =
        list.any { it.message.contains(message) && it.level.equals(level) }
}
