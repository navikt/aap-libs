package no.nav.aap.kafka.streams.v2.processor.state

import net.logstash.logback.argument.StructuredArguments.kv
import no.nav.aap.kafka.serde.json.Migratable
import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.StateStore
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory

class MigrateStateInitProcessor<T : Migratable>(
    private val ktable: KTable<T>,
    private val producer: Producer<String, T>,
    private val logValue: Boolean,
) : StateInitProcessor<T>(
    named = "migrate-${ktable.table.stateStoreName}",
    table = ktable,
) {
    override fun init(store: StateStore<T>) {
        store.forEach { key, value ->
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

    private companion object {
        private val secureLog = LoggerFactory.getLogger("secureLog")
    }
}
