package libs.kafka.processor

import libs.kafka.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import kotlin.jvm.optionals.getOrNull

internal interface KProcessor<T, U> {
    fun process(metadata: ProcessorMetadata, keyValue: KeyValue<String, T>): U
}

abstract class Processor<T, U>(private val named: String) : KProcessor<T, U> {
    internal companion object {
        internal fun <T, U> KStream<String, T>.addProcessor(processor: Processor<T, U>): KStream<String, U> =
            processValues(
                { processor.run { InternalProcessor() } },
                Named.`as`("stateless-operation-${processor.named}"),
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

            val metadata = ProcessorMetadata(
                topic = recordMeta.topic(),
                partition = recordMeta.partition(),
                offset = recordMeta.offset(),
                timestamp = record.timestamp(),
                systemTimeMs = context.currentSystemTimeMs(),
                streamTimeMs = context.currentStreamTimeMs(),
            )

            val valueToForward: U = process(
                metadata = metadata,
                keyValue = KeyValue(record.key(), record.value()),
            )

            context.forward(record.withValue(valueToForward))
        }
    }
}
