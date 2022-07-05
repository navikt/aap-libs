package no.nav.aap.kafka.serde.avro

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import no.nav.aap.kafka.SslConfig
import no.nav.aap.kafka.schemaregistry.SchemaRegistryConfig
import org.apache.avro.specific.SpecificRecord
import java.util.*

object AvroSerde {
    fun <T : SpecificRecord> specific(): SpecificAvroSerde<T> = SpecificAvroSerde<T>().apply {

        // configureres før appen starter og kan ikke hentes fra KStreamsConfig eller KafkaConfig.
        val schemaRegistry: Properties = SchemaRegistryConfig(
            url = System.getenv("KAFKA_SCHEMA_REGISTRY"),
            user = System.getenv("KAFKA_SCHEMA_REGISTRY_USER"),
            password = System.getenv("KAFKA_SCHEMA_REGISTRY_PASSWORD"),
        ).properties()

        // configureres før appen starter og kan ikke hentes fra KStreamsConfig eller KafkaConfig.
        val ssl: Properties = SslConfig(
            truststorePath = System.getenv("KAFKA_TRUSTSTORE_PATH"),
            keystorePath = System.getenv("KAFKA_KEYSTORE_PATH"),
            credstorePsw = System.getenv("KAFKA_CREDSTORE_PASSWORD")
        ).properties()

        val avroProperties = schemaRegistry + ssl
        val avroConfig = avroProperties.map { it.key.toString() to it.value.toString() }
        configure(avroConfig.toMap(), false)
    }

    fun generic() = GenericAvroSerde()
}
