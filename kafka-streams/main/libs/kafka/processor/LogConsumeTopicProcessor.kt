package libs.kafka.processor

import libs.kafka.KeyValue
import libs.kafka.Topic
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val log: Logger = LoggerFactory.getLogger("secureLog")

internal class LogConsumeTopicProcessor<T>(
    private val topic: Topic<T & Any>,
    namedSuffix: String = "",
) : Processor<T, T>("log-consume-${topic.name}$namedSuffix") {

    override fun process(metadata: ProcessorMetadata, keyValue: KeyValue<String, T>): T {
        log.trace(
            "Konsumerer Topic ${metadata.topic}",
            kv("key", keyValue.key),
            kv("topic", metadata.topic),
            kv("partition", metadata.partition),
            kv("offset", metadata.offset),
            if (topic.logValues) kv("value", keyValue.value) else null,
        )

        return keyValue.value
    }
}
