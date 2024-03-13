package no.nav.aap.kafka.serde.avro

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import no.nav.aap.kafka.schemaregistry.SchemaRegistryConfig
import no.nav.aap.kafka.streams.v2.config.SslConfig
import org.apache.avro.specific.SpecificRecord

object AvroSerde {
    fun <T : SpecificRecord> specific(
        schema: SchemaRegistryConfig = SchemaRegistryConfig.DEFAULT,
        ssl: SslConfig = SslConfig.DEFAULT,
    ): SpecificAvroSerde<T> = SpecificAvroSerde<T>().apply {
        val properties = schema.properties() + ssl.properties()
        val serdeConfig = properties.mapKeys { it.key.toString() }
        configure(serdeConfig, false)
    }

    fun generic() = GenericAvroSerde()
}
