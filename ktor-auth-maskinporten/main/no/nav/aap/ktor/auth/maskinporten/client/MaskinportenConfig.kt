package no.nav.aap.ktor.auth.maskinporten.client

import com.nimbusds.jose.jwk.RSAKey

data class MaskinportenConfig(
    val tokenEndpointUrl: String,
    val clientId: String,
    val privateKey: RSAKey,
    val scope: String,
    val resource: String,
    val issuer: String,
    val expireAfterSec: Long = 120
) {
    init {
        require(expireAfterSec <= 120) { "Maskinporten allows a maximum of 120 seconds expiry" }
    }
}
