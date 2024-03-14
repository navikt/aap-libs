package libs.kafka.serde

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer

object StringSerde : StreamSerde<String> {
    private val internalSerde = Serdes.StringSerde()
    override fun serializer(): Serializer<String> = internalSerde.serializer()
    override fun deserializer(): Deserializer<String> = internalSerde.deserializer()
}
