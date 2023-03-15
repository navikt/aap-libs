package no.nav.aap.kafka.streams.v2

import org.apache.kafka.streams.KeyValue

data class StreamsPair<L, R>(
    val left: L,
    val right: R,
)

data class KeyValue<K, V>(
    val key: K,
    val value: V,
) {
    internal fun toInternalKeyValue(): KeyValue<K, V> = KeyValue(key, value)
}
