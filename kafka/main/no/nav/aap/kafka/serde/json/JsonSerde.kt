package no.nav.aap.kafka.serde.json

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.logstash.logback.argument.StructuredArguments.kv
import net.logstash.logback.argument.StructuredArguments.raw
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
    ) = object : Serde<V> {
        override fun serializer(): Serializer<V> = JacksonSerializer()

        override fun deserializer(): Deserializer<V> =
            JacksonMigrationDeserializer(
                prevKClass = V_PREV::class,
                kclass = V::class,
                dtoVersion = dtoVersion,
                migrate = migrate,
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

interface Migratable {
    fun markerSomMigrertAkkuratNå()
    fun erMigrertAkkuratNå(): Boolean
}

class JacksonMigrationDeserializer<T_PREV : Any, T : Migratable>(
    private val prevKClass: KClass<T_PREV>,
    private val kclass: KClass<T>,
    private val dtoVersion: Int,
    private val migrate: (T_PREV) -> T,
) : Deserializer<T> {
    private val previousDtoVersion = dtoVersion - 1

    override fun deserialize(topic: String, data: ByteArray?): T? =
        data?.let { byteArray ->
            val json = jackson.readTree(byteArray)
            val version = json.get("version")

            runCatching {
                require(version.intValue() == dtoVersion) { "dto er ikke siste version $dtoVersion" }
                jackson.readValue(byteArray, kclass.java)
            }.getOrElse {
                require(version == null || version.intValue() == previousDtoVersion) {
                    "forrige dto er ikke forrige version $previousDtoVersion"
                }

                migrate(jackson.readValue(byteArray, prevKClass.java))
                    .also { it.markerSomMigrertAkkuratNå() }
                    .also {
                        secureLog.trace(
                            "Migrerte ved deserialisering",
                            kv("topic", topic),
                            kv("fra_versjon", version),
                            kv("til_versjon", dtoVersion),
                            raw("fra_data", byteArray.decodeToString()),
                            raw("til_data", jackson.writeValueAsString(it)) // fjern for optimalisering
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
