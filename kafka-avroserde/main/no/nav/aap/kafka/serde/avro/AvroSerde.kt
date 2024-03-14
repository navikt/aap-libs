package no.nav.aap.kafka.serde.avro

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import no.nav.aap.kafka.streams.v2.config.StreamsConfig
import no.nav.aap.kafka.streams.v2.serde.StreamSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

object AvroSerde {
    fun <T : SpecificRecord> specific(
        config: StreamsConfig,
    ): StreamSerde<T> = object : StreamSerde<T> {
        private val internal = SpecificAvroSerde<T>().apply {
            val schemaProperties = config.schemaRegistry?.properties() ?: error("missing required schema config")
            val sslProperties = config.ssl?.properties() ?: error("missing required ssl config")
            val properties = schemaProperties + sslProperties
            val serdeConfig = properties.mapKeys { it.key.toString() }
            configure(serdeConfig, false)
        }

        override fun serializer(): Serializer<T> = internal.serializer()
        override fun deserializer(): Deserializer<T> = internal.deserializer()
    }

    fun generic() = GenericAvroSerde()
}
