package no.nav.aap.kafka.streams.store

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

fun <V> ReadOnlyKeyValueStore<String, V>.allValues(): List<V> =
    all().use { it.asSequence().map(KeyValue<String, V>::value).toList() }
