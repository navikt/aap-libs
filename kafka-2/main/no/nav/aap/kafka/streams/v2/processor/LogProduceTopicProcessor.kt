package no.nav.aap.kafka.streams.v2.processor

import net.logstash.logback.argument.StructuredArguments.kv
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class LogProduceTopicProcessor<K, V>(
    private val logValue: Boolean = false,
) : FixedKeyProcessor<K, V, V> {
    private lateinit var context: FixedKeyProcessorContext<K, V>
    override fun init(ctxt: FixedKeyProcessorContext<K, V>) = let { context = ctxt }
    override fun close() {}
    override fun process(record: FixedKeyRecord<K, V>) {
        context.recordMetadata().ifPresentOrElse({ metadata ->
            log.trace(
                "Produserer til Topic ${metadata.topic()}",
                kv("key", record.key()),
                kv("topic", metadata.topic()),
                kv("partition", metadata.partition()),
                if (logValue) kv("value", record.value()) else null,
            )
        }, {
            log.warn("No metadata found for context in LogProduceTopicProcessor node: $context")
        })

        context.forward(record)
    }
}

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal fun <K, V> KStream<K, V>.logProduced(
    named: String,
    logValues: Boolean,
): KStream<K, V> = processValues(
    { LogProduceTopicProcessor<K, V>(logValues) },
    Named.`as`("log-$named")
)
