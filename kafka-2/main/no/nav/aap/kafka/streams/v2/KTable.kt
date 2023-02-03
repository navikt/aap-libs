package no.nav.aap.kafka.streams.v2

class KTable<T>(
    val table: Table<T>,
    val internalTable: org.apache.kafka.streams.kstream.KTable<String, T>,
)
