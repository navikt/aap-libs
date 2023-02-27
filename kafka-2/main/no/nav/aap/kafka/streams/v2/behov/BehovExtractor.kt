package no.nav.aap.kafka.streams.v2.behov

interface BehovExtractor<out JSON> {
    fun toJson(): JSON
}

interface Behov<in BEHOV_VISITOR> {
    fun accept(visitor: BEHOV_VISITOR)
}
