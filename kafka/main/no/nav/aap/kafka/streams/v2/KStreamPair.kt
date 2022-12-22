package no.nav.aap.kafka.streams.v2

data class KStreamPair<L, R>(
    val left: L,
    val right: R
)
