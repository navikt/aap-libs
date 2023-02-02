package no.nav.aap.kafka.streams.v2

class KTable<T>(
    val topic: Topic<T>,
    val table: org.apache.kafka.streams.kstream.KTable<String, T>,
)
