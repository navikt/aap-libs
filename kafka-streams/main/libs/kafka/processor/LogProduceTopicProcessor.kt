package libs.kafka.processor

import libs.kafka.KeyValue
import libs.kafka.Topic
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class LogProduceTopicProcessor<T> internal constructor(
    named: String,
    private val topic: Topic<T & Any>,
) : Processor<T, T>(named) {

    override fun process(metadata: ProcessorMetadata, keyValue: KeyValue<String, T>): T {
        log.trace(
            "Produserer til Topic ${topic.name}",
            kv("key", keyValue.key),
            kv("source_topic", metadata.topic),
            kv("topic", topic.name),
            kv("partition", metadata.partition),
            if (topic.logValues) kv("value", keyValue.value) else null,
        )
        return keyValue.value
    }
}

private val log: Logger = LoggerFactory.getLogger("secureLog")
