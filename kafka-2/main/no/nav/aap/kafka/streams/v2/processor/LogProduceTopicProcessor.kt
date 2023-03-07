package no.nav.aap.kafka.streams.v2.processor

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.streams.v2.KeyValue
import no.nav.aap.kafka.streams.v2.Topic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class LogProduceTopicProcessor<T> internal constructor(
    named: String,
    private val topic: Topic<T & Any>,
    private val logValue: Boolean,
) : Processor<T, T>(named) {

    override fun process(metadata: ProcessorMetadata, keyValue: KeyValue<String, T>): T {
        log.trace(
            "Produserer til Topic ${topic.name}",
            kv("key", keyValue.key),
            kv("source_topic", metadata.topic),
            kv("topic", topic.name),
            kv("partition", metadata.partition),
            if (logValue) kv("value", keyValue.value) else null,
        )
        return keyValue.value
    }
}

private val log: Logger = LoggerFactory.getLogger("secureLog")
