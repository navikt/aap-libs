package no.nav.aap.kafka.streams.v2.processor

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.streams.v2.KeyValue
import no.nav.aap.kafka.streams.v2.Topic
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal class LogConsumeTopicProcessor<T>(
    named: String,
    private val logValue: Boolean = false
) : KProcessor<T, T>(named) {
    override fun process(metadata: KMetadata, keyValue: KeyValue<String, T>): T {
        log.trace(
            "Konsumerer Topic ${metadata.topic}",
            kv("key", keyValue.key),
            kv("topic", metadata.topic),
            kv("partition", metadata.partition),
            kv("offset", metadata.offset),
            if (logValue) kv("value", keyValue.value) else null,
        )
        return keyValue.value
    }
}
