package no.nav.aap.kafka.streams.transformer

import net.logstash.logback.argument.StructuredArguments.kv
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal class LogConsumeTopic<K, V>(
    private val message: String,
    private val logValue: Boolean = false,
) : FixedKeyProcessor<K, V, V> {
    private lateinit var context: FixedKeyProcessorContext<K, V>
    override fun init(ctxt: FixedKeyProcessorContext<K, V>) = let { context = ctxt }
    override fun close() {}
    override fun process(record: FixedKeyRecord<K, V>) {
        log.trace(
            message,
            kv("key", record.key()),
            kv("topic", context.recordMetadata().get().topic()),
            kv("partition", context.recordMetadata().get().partition()),
            kv("offset", context.recordMetadata().get().offset()),
            if (logValue) kv("value", record.value()) else null,
        )
        context.forward(record)
    }
}
