package libs.kafka

data class Table<T : Any>(
    val sourceTopic: Topic<T>,
    val stateStoreName: String = "${sourceTopic.name}-state-store"
) {
    val sourceTopicName: String
        get() = sourceTopic.name
}
