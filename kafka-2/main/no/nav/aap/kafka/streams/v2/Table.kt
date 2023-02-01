package no.nav.aap.kafka.streams.v2

data class Table<V>(
    val source: Topic<V>,
    val global: Boolean = false,
    val stateStoreName: String = "${source.name}-state-store"
) {
    val name: String get() = source.name
}
