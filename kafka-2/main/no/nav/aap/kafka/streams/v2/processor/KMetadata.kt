package no.nav.aap.kafka.streams.v2.processor

data class KMetadata(
    val topic: String,
    val partition: Int,
    val offset: Long,
)
