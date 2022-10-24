package no.nav.aap.ktor.client

import java.time.Instant

data class Token(val expires_in: Long, val access_token: String) {
    private val expiry = Instant.now().plusSeconds(expires_in - LEEWAY_SECONDS)

    fun hasExpired() = expiry < Instant.now()

    private companion object {
        const val LEEWAY_SECONDS = 60
    }
}
