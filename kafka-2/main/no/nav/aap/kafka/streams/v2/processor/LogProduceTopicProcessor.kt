package no.nav.aap.kafka.streams.v2.processor

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.streams.v2.KeyValue
import no.nav.aap.kafka.streams.v2.processor.KProcessor.Companion.addProcessor
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class LogProduceTopicProcessor<T> internal constructor(
    named: String,
    private val logValue: Boolean = false,
) : KProcessor<T, T>(named) {

    override fun process(metadata: KMetadata, keyValue: KeyValue<String, T>): T {
        log.trace(
            "Produserer til Topic ${metadata.topic}",
            kv("key", keyValue.key),
            kv("topic", metadata.topic),
            kv("partition", metadata.partition),
            if (logValue) kv("value", keyValue.value) else null,
        )
        return keyValue.value
    }
}

private val log: Logger = LoggerFactory.getLogger("secureLog")