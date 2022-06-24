package no.nav.aap.kafka.streams.transformer

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.streams.Table
import no.nav.aap.kafka.streams.Topic
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.processor.ProcessorContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

internal class TraceLogTransformer<K, V>(
    private val message: String,
    private val logValue: Boolean = false,
    private val table: Table<V>? = null,
    private val sinkTopic: Topic<V>? = null,
    private val level: Level = Level.TRACE,
) : ValueTransformerWithKey<K, V, V> {
    private val log: Logger = LoggerFactory.getLogger("secureLog")

    private lateinit var context: ProcessorContext

    override fun init(context: ProcessorContext) {
        this.context = context
    }

    override fun transform(key: K, value: V): V {
        val kvKey = kv("key", key)
        val vkTopic = kv("topic", sinkTopic?.name ?: context.topic())
        val kvTable = table?.let { kv("table", it.name) }
        val kvStore = table?.let { kv("store", it.stateStoreName) }
        val kvValue = if (logValue) kv("value", value) else null
        val kvPartition = kv("partition", context.partition())
        val kvOffset = kv("offset", context.offset())

        when (level) {
            Level.TRACE -> log.trace(message, kvKey, vkTopic, kvTable, kvStore, kvValue, kvPartition, kvOffset)
            Level.DEBUG -> log.debug(message, kvKey, vkTopic, kvTable, kvStore, kvValue, kvPartition, kvOffset)
            else -> log.info(message, kvKey, vkTopic, kvTable, kvStore, kvValue, kvPartition, kvOffset)
        }

        return value
    }

    override fun close() {}
}
