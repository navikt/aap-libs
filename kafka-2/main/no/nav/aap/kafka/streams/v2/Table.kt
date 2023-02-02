package no.nav.aap.kafka.streams.v2

data class Table<T>(
    val source: Topic<T>,
    val global: Boolean = false,
    val stateStoreName: String = "${source.name}-state-store"
) {
    val name: String get() = source.name
}
