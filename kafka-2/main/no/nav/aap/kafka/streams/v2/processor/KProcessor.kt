package no.nav.aap.kafka.streams.v2.processor

import no.nav.aap.kafka.streams.v2.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import kotlin.jvm.optionals.getOrNull

internal interface KProcess<T, U> {
    fun process(metadata: KMetadata, keyValue: KeyValue<String, T>): U
}

abstract class KProcessor<T, U>(private val named: String) : KProcess<T, U> {
    internal companion object {
        internal fun <T, U> KStream<String, T>.addProcessor(processor: KProcessor<T, U>): KStream<String, U> =
            processValues(
                { processor.run { InternalProcessor() } },
                Named.`as`(processor.named),
            )
    }

    private inner class InternalProcessor : FixedKeyProcessor<String, T, U> {
        private lateinit var context: FixedKeyProcessorContext<String, U>

        override fun init(context: FixedKeyProcessorContext<String, U>) {
            this.context = context
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
                keyValue = KeyValue(record.key(), record.value()),
            )

            context.forward(record.withValue(valueToForward))
        }
    }
}
