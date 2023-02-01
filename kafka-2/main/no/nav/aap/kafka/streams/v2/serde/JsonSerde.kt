package no.nav.aap.kafka.streams.v2.serde

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.logstash.logback.argument.StructuredArguments.kv
import net.logstash.logback.argument.StructuredArguments.raw
import no.nav.aap.kafka.serde.json.Migratable
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import kotlin.reflect.KClass

object JsonSerde {
    inline fun <reified V : Any> jackson() = object : Serde<V> {
        override fun serializer(): Serializer<V> = JacksonSerializer()
        override fun deserializer(): Deserializer<V> = JacksonDeserializer(V::class)
    }

    /**
     * OBS!
     * Hvis forrige dto ikke har versjon, og neste versjon kan parses til forrige så vil den ikke migrere.
     * Løsning: Ikke introduser initiell versjon før man har en "breaking" change.
     * Alle etterfølgende versjoner trenger ikke være "breaking".
     */
    inline fun <reified V : Migratable, reified V_PREV : Any> jackson(
        dtoVersion: Int,
        noinline migrate: (V_PREV) -> V,
        noinline versionSupplier: (JsonNode) -> Int? = { json ->
            json.get("version")?.takeIf { it.isNumber }?.intValue()
        },
    ) = object : Serde<V> {
        override fun serializer(): Serializer<V> = JacksonSerializer()

        override fun deserializer(): Deserializer<V> =
            JacksonMigrationDeserializer(
                prevKClass = V_PREV::class,
                kclass = V::class,
                dtoVersion = dtoVersion,
                migrate = migrate,
                versionSupplier = versionSupplier,
            )
    }
}

class JacksonSerializer<T : Any> : Serializer<T> {
    private val jackson: ObjectMapper = jacksonObjectMapper().apply {
        registerModule(JavaTimeModule())
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    override fun serialize(topic: String, data: T?): ByteArray? = data?.let { jackson.writeValueAsBytes(it) }
}

class JacksonDeserializer<T : Any>(private val kclass: KClass<T>) : Deserializer<T> {
    private val jackson: ObjectMapper = jacksonObjectMapper().apply {
        registerModule(JavaTimeModule())
        disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }

    override fun deserialize(topic: String, data: ByteArray?): T? = data?.let { jackson.readValue(it, kclass.java) }
}

class JacksonMigrationDeserializer<T_PREV : Any, T : Migratable>(
    private val prevKClass: KClass<T_PREV>,
    private val kclass: KClass<T>,
    private val dtoVersion: Int,
    private val migrate: (T_PREV) -> T,
    private val logValues: Boolean = false,
    private val versionSupplier: (JsonNode) -> Int?,
) : Deserializer<T> {
    private val previousDtoVersion = dtoVersion - 1

    override fun deserialize(topic: String, data: ByteArray?): T? =
        data?.let { byteArray ->
            val json: JsonNode = jackson.readTree(byteArray)

            when (val version = versionSupplier(json)) {
                null, previousDtoVersion -> deserializeForMigration(byteArray, topic, version)
                dtoVersion -> deserializeDto(topic, byteArray, json)
                else -> serdeError(
                    """
                      Klarte ikke deserialisere data på topic $topic
                      Gjeldende versjon: $dtoVersion
                      Forrige versjon: $previousDtoVersion
                      Versjon i data: $version
                    """.trimIndent()
                )
            }
        }

    private fun deserializeDto(topic: String, bytes: ByteArray, json: JsonNode): T =
        try {
            jackson.readValue(bytes, kclass.java)
        } catch (e: Exception) {
            secureLog.error(
                """
                    Klarte ikke deserializere data på topic $topic
                    Versjon i data og dataklasse ${kclass.java.name} = $dtoVersion
                    Migrering kan være nødvendig.
                    Data: $json
                """.trimIndent()
            )
            serdeError("Klarte ikke deserializere data på topic $topic", e)
        }

    private fun deserializeForMigration(bytes: ByteArray, topic: String, version: Int?): T {
        val previousDto = try {
            jackson.readValue(bytes, prevKClass.java)
        } catch (e: Exception) {
            secureLog.error(
                """
                    Klarte ikke deserializere data på topic $topic til forrige dto
                    Forrige versjon = $previousDtoVersion
                    Versjon i data = $version
                """.trimIndent()
            )
            serdeError("Klarte ikke deserializere data på topic $topic til forrige dto", e)
        }

        return migrate(previousDto)
            .apply { markerSomMigrertAkkuratNå() }
            .also { migratedDto ->
                if (logValues) {
                    secureLog.trace(
                        "Migrerte ved deserialisering",
                        kv("topic", topic),
                        kv("fra_versjon", version),
                        kv("til_versjon", dtoVersion),
                        raw("fra_data", bytes.decodeToString()),
                        raw("til_data", jackson.writeValueAsString(migratedDto))
                    )
                } else {
                    secureLog.trace(
                        "Migrerte ved deserialisering",
                        kv("topic", topic),
                        kv("fra_versjon", version),
                        kv("til_versjon", dtoVersion),
                    )
                }
            }
    }

    private companion object {
        private val secureLog = LoggerFactory.getLogger("secureLog")
        private val jackson: ObjectMapper = jacksonObjectMapper().apply {
            registerModule(JavaTimeModule())
            disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        }
    }
}

class JsonSerdeException(msg: String, cause: Throwable) : RuntimeException(msg, cause)

private fun serdeError(message: Any): Nothing = throw JsonSerdeException(message.toString(), RuntimeException())
private fun serdeError(message: Any, cause: Throwable): Nothing = throw JsonSerdeException(message.toString(), cause)
