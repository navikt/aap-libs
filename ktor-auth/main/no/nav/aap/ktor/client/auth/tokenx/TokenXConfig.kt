package no.nav.aap.ktor.client.auth.tokenx

import no.nav.aap.ktor.client.auth.util.JwtConfig
import no.nav.aap.ktor.client.auth.util.getEnvVar
import java.net.URL
import java.time.Instant
import java.util.*

data class TokenXConfig(
    val clientId: String = getEnvVar("TOKEN_X_CLIENT_ID"),
    val privateKey: String = getEnvVar("TOKEN_X_PRIVATE_JWK"),
    val tokenEndpoint: String = getEnvVar("TOKEN_X_TOKEN_ENDPOINT"),
    val jwksUrl: String = getEnvVar("TOKEN_X_JWKS_URI"),
    val issuer: String = getEnvVar("TOKEN_X_ISSUER"),
)

internal fun TokenXConfig.toJwtConfig() = JwtConfig(
    privateKey = privateKey,
    claimset = mapOf(
        "aud" to tokenEndpoint,
        "iss" to clientId,
        "iat" to Date(),
        "exp" to Date.from(Instant.now().plusSeconds(120)),
        "sub" to clientId,
        "jti" to UUID.randomUUID().toString(),
        "nbf" to Date()
    )
)
