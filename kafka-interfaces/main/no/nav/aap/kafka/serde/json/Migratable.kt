package no.nav.aap.kafka.serde.json

interface Migratable {
    fun markerSomMigrertAkkuratNå()
    fun erMigrertAkkuratNå(): Boolean
}
