package no.nav.aap.kafka.streams.v2.processor

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal class LogConsumeTopicProcessor<K, V>(
    private val logValue: Boolean = false,
) : FixedKeyProcessor<K, V, V> {
    private lateinit var context: FixedKeyProcessorContext<K, V>
    override fun init(ctxt: FixedKeyProcessorContext<K, V>) = let { context = ctxt }
    override fun close() {}
    override fun process(record: FixedKeyRecord<K, V>) {
        context.recordMetadata().ifPresentOrElse(
            { metadata ->
                log.trace(
                    "Konsumerer Topic ${metadata.topic()}",
                    kv("key", record.key()),
                    kv("topic", metadata.topic()),
                    kv("partition", metadata.partition()),
                    kv("offset", metadata.offset()),
                    if (logValue) kv("value", record.value()) else null,
                )
            },
            {
                log.warn("No metadata found for context in LogConsumeTopicProfcessor node: $context")
            }
        )

        context.forward(record)
    }
}

internal fun <K, V> KStream<K, V?>.logConsumed(
    topic: Topic<V>,
    logValues: Boolean = false,
) = processValues(
    { LogConsumeTopicProcessor(logValues) },
    Named.`as`("log-consume-${topic.name}")
)
