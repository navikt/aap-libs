package no.nav.aap.kafka.streams.transformer

import net.logstash.logback.argument.StructuredArguments.kv
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.processor.ProcessorContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal class LogConsumeTopic<K, V>(
    private val message: String,
    private val logValue: Boolean = false,
) : ValueTransformerWithKey<K, V, V> {
    private lateinit var context: ProcessorContext
    override fun init(ctxt: ProcessorContext) = let { context = ctxt }
    override fun close() {}
    override fun transform(key: K, value: V): V = value.also {
        log.trace(
            message,
            kv("key", key),
            kv("topic", context.topic()),
            kv("partition", context.partition()),
            kv("offset", context.offset()),
            if (logValue) kv("value", value) else null,
        )
    }
}
