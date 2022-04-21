package no.nav.aap.kafka.serde.avro

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import no.nav.aap.kafka.KafkaConfig
import no.nav.aap.kafka.plus
import org.apache.avro.specific.SpecificRecord

object AvroSerde {
    fun <T : SpecificRecord> specific(config: KafkaConfig): SpecificAvroSerde<T> = SpecificAvroSerde<T>().apply {
        val avroProperties = config.schemaRegistry + config.ssl
        val avroConfig = avroProperties.map { it.key.toString() to it.value.toString() }
        configure(avroConfig.toMap(), false)
    }

    fun generic() = GenericAvroSerde()
}
