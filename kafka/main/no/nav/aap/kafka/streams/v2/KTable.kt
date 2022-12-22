package no.nav.aap.kafka.streams.v2

import no.nav.aap.kafka.streams.Topic

class KTable<V : Any>(
    val topic: Topic<V>,
    val table: org.apache.kafka.streams.kstream.KTable<String, V>,
)
