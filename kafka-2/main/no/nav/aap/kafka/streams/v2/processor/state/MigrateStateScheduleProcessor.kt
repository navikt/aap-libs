package no.nav.aap.kafka.streams.v2.processor.state

import net.logstash.logback.argument.StructuredArguments.*
import no.nav.aap.kafka.serde.json.Migratable
import no.nav.aap.kafka.streams.v2.KTable
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import org.slf4j.LoggerFactory
import kotlin.time.Duration

class MigrateStateScheduleProcessor<T: Migratable>(
    named: String,
    interval: Duration,
    private val ktable: KTable<T>,
    private val producer: Producer<String, T>,
    private val logValue: Boolean,
    ) : StateScheduleProcessor<T>(
    named = named,
    table = ktable,
    interval = interval,
) {
    override fun schedule(timestamp: Long, store: TimestampedKeyValueStore<String, T>) {
        store.all().use {
            it.asSequence()
                .forEach { record ->
                    val (key, value) = record.key to record.value.value()
                    if (value.erMigrertAkkuratNå()) {
                        val migrertRecord = ProducerRecord(ktable.table.sourceTopicName, key, value)
                        producer.send(migrertRecord) { meta, error ->
                            if (error == null) {
                                secureLog.trace(
                                    "Migrerte state store",
                                    kv("key", key),
                                    kv("topic", ktable.table.sourceTopicName),
                                    kv("store", ktable.table.stateStoreName),
                                    kv("partition", meta.partition()),
                                    kv("offset", meta.offset()),
                                    if (logValue) kv("value", value) else null
                                )
                            } else {
                                secureLog.error("klarte ikke sende migrert dto", error)
                            }
                        }
                    }
                }
        }
    }

    private companion object {
        private val secureLog = LoggerFactory.getLogger("secureLog")
    }
}
