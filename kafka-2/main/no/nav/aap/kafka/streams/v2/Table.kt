package no.nav.aap.kafka.streams.v2

data class Table<T : Any>(
    val sourceTopic: Topic<T>,
    val global: Boolean = false,
    val stateStoreName: String = "${sourceTopic.name}-state-store"
) {
    val sourceTopicName: String get() = sourceTopic.name
}
