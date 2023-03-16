package no.nav.aap.kafka.streams.v2.processor

import no.nav.aap.kafka.streams.v2.KeyValue
import no.nav.aap.kafka.streams.v2.Topic

/**
 * @param timestamp: The current timestamp in the producers environment
 * @param systemTimeMs: Current system timestamp (wall-clock-time)
 * @param streamTimeMs: The largest timestamp seen so far, and it only moves forward
 */
data class ProcessorMetadata(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val systemTimeMs: Long,
    val streamTimeMs: Long,
)

//internal class MetadataProcessor<T : Any>(
//    topic: Topic<T>,
//) : Processor<T?, Pair<KeyValue<String, T?>, ProcessorMetadata>>(
//    "from-${topic.name}-enrich-metadata",
//) {
//    override fun process(
//        metadata: ProcessorMetadata,
//        keyValue: KeyValue<String, T?>,
//    ): Pair<KeyValue<String, T?>, ProcessorMetadata> =
//        keyValue to metadata
//}

internal class MetadataProcessor<T>(
    topic: Topic<T & Any>,
) : Processor<T, Pair<KeyValue<String, T>, ProcessorMetadata>>(
    "from-${topic.name}-enrich-metadata",
) {
    override fun process(
        metadata: ProcessorMetadata,
        keyValue: KeyValue<String, T>,
    ): Pair<KeyValue<String, T>, ProcessorMetadata> =
        keyValue to metadata
}
