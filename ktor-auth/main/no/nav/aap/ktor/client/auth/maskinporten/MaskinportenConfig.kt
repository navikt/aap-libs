package no.nav.aap.ktor.client.auth.maskinporten

import no.nav.aap.ktor.client.auth.util.JwtConfig
import java.time.Instant
import java.util.*

data class MaskinportenConfig(
    val tokenEndpointUrl: String,
    val clientId: String,
    val privateKey: String,
    val scope: String,
    val resource: String,
    val issuer: String
)

internal fun MaskinportenConfig.toJwtConfig() = JwtConfig(
    privateKey = privateKey,
    claimset = mapOf(
        "scope" to scope,
        "resource" to resource,
        "aud" to issuer,
        "iss" to clientId,
        "iat" to Date(),
        "exp" to Date.from(Instant.now().plusSeconds(120))
    )
)
