package no.nav.aap.kafka.streams.transformer

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.streams.Table
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.processor.ProcessorContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal class LogProduceTable<K, V>(
    private val message: String,
    private val table: Table<V>,
    private val logValue: Boolean = false,
) : ValueTransformerWithKey<K, V, V> {
    private lateinit var context: ProcessorContext
    override fun init(ctxt: ProcessorContext) = let { context = ctxt }
    override fun close() {}
    override fun transform(key: K, value: V): V = value.also {
        log.trace(
            message,
            kv("key", key),
            kv("table", table.name),
            kv("store", table.stateStoreName),
            kv("partition", context.partition()),
            if (logValue) kv("value", value) else null,
        )
    }
}
