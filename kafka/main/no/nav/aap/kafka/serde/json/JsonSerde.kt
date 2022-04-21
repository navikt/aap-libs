package no.nav.aap.kafka.serde.json

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import kotlin.reflect.KClass

object JsonSerde {
    inline fun <reified V : Any> jackson() = object : Serde<V> {
        override fun serializer(): Serializer<V> = JacksonSerializer()
        override fun deserializer(): Deserializer<V> = JacksonDeserializer(V::class)
    }
}

class JacksonDeserializer<T : Any>(private val kclass: KClass<T>) : Deserializer<T> {
    private val jackson: ObjectMapper = jacksonObjectMapper().apply {
        registerModule(JavaTimeModule())
        disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }

    override fun deserialize(topic: String, data: ByteArray?): T? = jackson.readValue(data, kclass.java)
}

class JacksonSerializer<T : Any> : Serializer<T> {
    private val jackson: ObjectMapper = jacksonObjectMapper().apply {
        registerModule(JavaTimeModule())
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    override fun serialize(topic: String, data: T?): ByteArray? = data?.let { jackson.writeValueAsBytes(it) }
}
