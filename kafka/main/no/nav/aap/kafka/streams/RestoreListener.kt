package no.nav.aap.kafka.streams

import net.logstash.logback.argument.StructuredArguments.kv
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.processor.StateRestoreListener
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

private val log = LoggerFactory.getLogger("kafka")


internal class RestoreListener : StateRestoreListener {
    private val startMs = AtomicLong()

    override fun onRestoreStart(partition: TopicPartition, storeName: String, startOffset: Long, endOffset: Long) {
        startMs.set(System.currentTimeMillis())
    }

    override fun onRestoreEnd(partition: TopicPartition, storeName: String, totalRestored: Long) {
        val duration = Duration.ofMillis(System.currentTimeMillis() - startMs.getAndSet(Long.MAX_VALUE))

        log.info(
            "Gjennopprettet #$totalRestored state etter $duration",
            kv("partition", partition.partition()),
            kv("topic", partition.topic()),
            kv("store", storeName),
        )
    }

    override fun onBatchRestored(partition: TopicPartition, storeName: String, endOffset: Long, numRestored: Long) {
        // This is very noisy, Don't log anything
    }
}