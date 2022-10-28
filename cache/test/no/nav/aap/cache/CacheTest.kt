package no.nav.aap.cache

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

internal class CacheTest {

    @Test
    fun `Returnerer cached verdi`() {
        val cache = Cache<String, String>(ChronoUnit.MINUTES.duration)
        val nøkkel = "nøkkel"
        val verdi = "verdi"

        cache[nøkkel] = verdi

        assertEquals(verdi, cache[nøkkel])
    }

    @Test
    fun `Returnerer ikke gammel cached verdi`() {
        val cache = Cache<String, String>()
        val nøkkel = "nøkkel"
        val verdi = "verdi"

        cache.set(nøkkel, verdi, Instant.now().minusMillis(1))

        assertNull(cache[nøkkel])
    }

    @Test
    fun `Overskriver gammel cached verdi med ny`() {
        val cache = Cache<String, String>(ChronoUnit.MINUTES.duration)
        val nøkkel = "nøkkel"
        val gammelVerdi = "gammelVerdi"
        val nyVerdi = "nyVerdi"

        cache[nøkkel] = gammelVerdi
        assertEquals(gammelVerdi, cache[nøkkel])

        cache[nøkkel] = nyVerdi
        assertEquals(nyVerdi, cache[nøkkel])
    }

    @Test
    fun `Gamle verdier fjernes før cachen sjekkes`() {
        val cache = Cache<String, String>()
        val nøkkel1 = "nøkkel1"
        val nøkkel2 = "nøkkel2"
        val nøkkel3 = "nøkkel3"
        val verdi1 = "verdi1"
        val verdi2 = "verdi2"
        val verdi3 = "verdi3"

        cache.set(nøkkel1, verdi1, Instant.now().plusSeconds(60))
        cache.set(nøkkel2, verdi2, Instant.now().minusMillis(1))
        cache.set(nøkkel3, verdi3, Instant.now().plusSeconds(60))

        assertEquals(verdi1, cache[nøkkel1])
        assertNull(cache[nøkkel2])
        assertEquals(verdi3, cache[nøkkel3])
    }

    @Test
    fun `Hele cachen tømmes ved kall mot clear() uavhengig av levetid`() {
        val cache = Cache<String, String>()
        val nøkkel1 = "nøkkel1"
        val nøkkel2 = "nøkkel2"
        val nøkkel3 = "nøkkel3"
        val verdi1 = "verdi1"
        val verdi2 = "verdi2"
        val verdi3 = "verdi3"

        cache.set(nøkkel1, verdi1, Instant.now().plusSeconds(60))
        cache.set(nøkkel2, verdi2, Instant.now().plusSeconds(60))
        cache.set(nøkkel3, verdi3, Instant.now().plusSeconds(60))

        cache.clear()

        assertNull(cache[nøkkel1])
        assertNull(cache[nøkkel2])
        assertNull(cache[nøkkel3])
    }

    @Test
    fun `Fjerner angitt nøkkel ved kall til remove(nøkkel) uavhengig av levetid`() {
        val cache = Cache<String, String>()
        val nøkkel = "nøkkel"
        val verdi = "verdi"

        cache.set(nøkkel, verdi, Instant.now().plusSeconds(60))

        cache.remove(nøkkel)

        assertNull(cache[nøkkel])
    }

    @Test
    fun `Fjerner ikke andre nøkler ved kall til remove(nøkkel)`() {
        val cache = Cache<String, String>()
        val nøkkel1 = "nøkkel1"
        val nøkkel2 = "nøkkel2"
        val nøkkel3 = "nøkkel3"
        val verdi1 = "verdi1"
        val verdi2 = "verdi2"
        val verdi3 = "verdi3"

        cache.set(nøkkel1, verdi1, Instant.now().plusSeconds(60))
        cache.set(nøkkel2, verdi2, Instant.now().plusSeconds(60))
        cache.set(nøkkel3, verdi3, Instant.now().plusSeconds(60))

        cache.remove(nøkkel2)

        assertEquals(verdi1, cache[nøkkel1])
        assertNull(cache[nøkkel2])
        assertEquals(verdi3, cache[nøkkel3])
    }
}
