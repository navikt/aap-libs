package no.nav.aap.kafka.streams.v2.processor

import no.nav.aap.kafka.streams.v2.KTable
import no.nav.aap.kafka.streams.v2.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import kotlin.jvm.optionals.getOrNull

internal interface KStoreProcess<T, U> {
    fun process(
        metadata: KMetadata,
        store: TimestampedKeyValueStore<String, T>,
        keyValue: KeyValue<String, T>,
    ): U
}

abstract class KStoreProcessor<T, U>(
    private val named: String,
    private val table: KTable<T>,
) : KStoreProcess<T, U> {
    internal companion object {
        internal fun <T, U> KStream<String, T>.addProcessor(
            processor: KStoreProcessor<T, U>
        ): KStream<String, U> = processValues(
            { processor.run(KStoreProcessor<T, U>::InternalProcessor) },
            Named.`as`(processor.named),
            processor.table.table.stateStoreName,
        )
    }

    private inner class InternalProcessor : FixedKeyProcessor<String, T, U> {
        private lateinit var context: FixedKeyProcessorContext<String, U>
        private lateinit var store: TimestampedKeyValueStore<String, T>

        override fun init(context: FixedKeyProcessorContext<String, U>) {
            this.context = context
            this.store = context.getStateStore(table.table.stateStoreName)
        }

        override fun process(record: FixedKeyRecord<String, T>) {
            val recordMeta = requireNotNull(context.recordMetadata().getOrNull()) {
                "Denne er bare null når man bruker punctuators. Det er feil å bruke denne klassen til punctuation."
            }

            val metadata = KMetadata(
                topic = recordMeta.topic(),
                partition = recordMeta.partition(),
                offset = recordMeta.offset()
            )

            val valueToForward: U = process(
                metadata = metadata,
                store = store,
                keyValue = KeyValue(record.key(), record.value()),
            )

            context.forward(record.withValue(valueToForward))
        }
    }
}
