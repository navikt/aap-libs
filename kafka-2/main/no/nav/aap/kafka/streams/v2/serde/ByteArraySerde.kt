package no.nav.aap.kafka.streams.v2.serde

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

object ByteArraySerde: StreamSerde<ByteArray> {
    private val internalSerde = org.apache.kafka.common.serialization.Serdes.ByteArraySerde()
    override fun serializer(): Serializer<ByteArray> = internalSerde.serializer()
    override fun deserializer(): Deserializer<ByteArray> = internalSerde.deserializer()

}
