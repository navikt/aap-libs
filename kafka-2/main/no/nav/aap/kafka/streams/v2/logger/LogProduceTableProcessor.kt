package no.nav.aap.kafka.streams.v2.logger

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.streams.v2.Table
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal class LogProduceTableProcessor<K, V>(
    private val table: Table<V>,
    private val logValue: Boolean,
) : FixedKeyProcessor<K, V, V> {
    private lateinit var context: FixedKeyProcessorContext<K, V>
    override fun init(ctxt: FixedKeyProcessorContext<K, V>) = let { context = ctxt }
    override fun close() {}
    override fun process(record: FixedKeyRecord<K, V>) {
        context.recordMetadata().ifPresentOrElse(
            { metadata ->
                log.trace(
                    "Produserer til KTable ${table.name}",
                    kv("key", record.key()),
                    kv("table", table.name),
                    kv("store", table.stateStoreName),
                    kv("partition", metadata.partition()),
                    if (logValue) kv("value", record.value()) else null,
                )
            },
            {
                log.warn("No metadata found for context in LogProduceTableProcessor node: $context")
            }
        )

        context.forward(record)
    }
}

internal fun <K, V> KStream<K, V>.logProduced(
    table: Table<V>,
    logValues: Boolean,
): KStream<K, V> = processValues(
    { LogProduceTableProcessor<K, V>(table, logValues) },
    Named.`as`("log-produced-${table.name}")
)
