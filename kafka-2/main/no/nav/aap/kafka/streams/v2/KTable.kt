package no.nav.aap.kafka.streams.v2

class KTable<V>(
    val topic: Topic<V>,
    val table: org.apache.kafka.streams.kstream.KTable<String, V>,
)
