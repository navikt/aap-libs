package no.nav.aap.kafka.serde.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

fun GenericRecord.generic(name: String): GenericRecord? = get(name) as GenericRecord?
fun GenericRecord.string(name: String) = get(name)?.toString()
fun GenericRecord.array(name: String) = get(name) as GenericData.Array<*>

inline fun <reified V : Enum<V>> GenericRecord.enum(name: String) = get(name)?.let { enumValueOf<V>(it.toString()) }
