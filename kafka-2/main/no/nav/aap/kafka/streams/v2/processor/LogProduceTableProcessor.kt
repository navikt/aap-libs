package no.nav.aap.kafka.streams.v2.processor

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.streams.v2.KeyValue
import no.nav.aap.kafka.streams.v2.Table
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal class LogProduceTableProcessor<T>(
    named: String,
    private val table: Table<T>,
    private val logValue: Boolean,
) : KProcessor<T, T>(named) {

    override fun process(metadata: KMetadata, keyValue: KeyValue<String, T>): T {
        log.trace(
            "Produserer til KTable ${table.name}",
            kv("key", keyValue.key),
            kv("table", table.name),
            kv("store", table.stateStoreName),
            kv("partition", metadata.partition),
            if (logValue) kv("value", keyValue.value) else null,
        )
        return keyValue.value
    }
}
