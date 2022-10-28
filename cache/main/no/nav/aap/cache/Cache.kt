package no.nav.aap.cache

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAmount
import java.time.temporal.TemporalUnit

class Cache<in NØKKEL, VERDI>(
    private val levetid: TemporalAmount = ChronoUnit.MINUTES.duration
) {
    private val cache = hashMapOf<NØKKEL, Element<NØKKEL, VERDI>>()
    private val timestampCache = sortedMapOf<Instant, List<Element<NØKKEL, VERDI>>>()

    private class Element<NØKKEL, VERDI>(
        val nøkkel: NØKKEL,
        val verdi: VERDI,
        val utløpstidspunkt: Instant
    )

    constructor(
        levetid: Long = 1,
        tidsenhet: TemporalUnit = ChronoUnit.MINUTES
    ) : this(Duration.of(levetid, tidsenhet))

    private fun clean() {
        timestampCache
            .headMap(Instant.now())
            .onEach { (_, liste) ->
                liste.forEach { element ->
                    cache.remove(element.nøkkel, element)
                }
            }
            .clear()
    }

    private fun setTimestampCache(utløpstidspunkt: Instant, element: Element<NØKKEL, VERDI>) {
        timestampCache.compute(utløpstidspunkt) { _, liste ->
            if (liste == null) return@compute arrayListOf(element)
            liste + element
        }
    }

    private fun removeTimestampCache(utløpstidspunkt: Instant, element: Element<NØKKEL, VERDI>) {
        timestampCache.computeIfPresent(utløpstidspunkt) { _, liste ->
            if (liste.count() == 1) return@computeIfPresent null
            liste - element
        }
    }

    @Synchronized
    operator fun get(nøkkel: NØKKEL): VERDI? {
        clean()
        return cache[nøkkel]?.verdi
    }

    @Synchronized
    operator fun set(nøkkel: NØKKEL, verdi: VERDI) {
        set(nøkkel, verdi, Instant.now().plus(levetid))
    }

    @Synchronized
    fun set(nøkkel: NØKKEL, verdi: VERDI, utløpstidspunkt: Instant) {
        clean()

        if (verdi == null) return

        val element: Element<NØKKEL, VERDI> = Element(nøkkel, verdi, utløpstidspunkt)
        cache[nøkkel] = element
        setTimestampCache(utløpstidspunkt, element)
    }

    @Synchronized
    fun clear() {
        timestampCache.clear()
        cache.clear()
    }

    @Synchronized
    fun isEmpty() = timestampCache.isEmpty() && cache.isEmpty()

    @Synchronized
    fun remove(nøkkel: NØKKEL) {
        cache.compute(nøkkel) { _, fjernetElement ->
            if (fjernetElement != null) {
                removeTimestampCache(fjernetElement.utløpstidspunkt, fjernetElement)
            }
            null
        }
    }
}
