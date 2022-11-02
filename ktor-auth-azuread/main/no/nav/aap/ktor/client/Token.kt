package no.nav.aap.ktor.client

import no.nav.aap.cache.Cache
import java.time.Instant

internal data class Token(val expires_in: Long, val access_token: String) {
    private val expiry: Instant = Instant.now().plusSeconds(expires_in - LEEWAY_SECONDS)

    internal fun addToCache(cache: Cache<String, Token>, cacheKey: String) {
        cache.set(cacheKey, this, expiry)
    }

    private companion object {
        const val LEEWAY_SECONDS = 60
    }
}
