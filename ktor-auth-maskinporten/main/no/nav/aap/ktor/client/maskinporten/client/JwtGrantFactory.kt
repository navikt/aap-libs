package no.nav.aap.ktor.client.maskinporten.client

import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.JWSHeader
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.SignedJWT
import java.time.Instant
import java.util.*

internal class JwtGrantFactory(private val config: MaskinportenConfig) {
    internal val jwt: String get() = signedJwt.serialize()

    private val signedJwt get() = SignedJWT(jwsHeader, jwtClaimSet).apply { sign(RSASSASigner(config.privateKey)) }
    private val jwsHeader get() = JWSHeader.Builder(JWSAlgorithm.RS256).keyID(config.privateKey.keyID).build()

    private val jwtClaimSet: JWTClaimsSet
        get() = JWTClaimsSet.Builder().apply {
            audience(config.issuer) // maskinporten is aud
            issuer(config.clientId) // this app is issuer
            issueTime(Date())
            expirationTime(Instant.now() + config.expireAfterSec)
            claim("scope", config.scope)
            claim("resource", config.resource)
        }.build()
}

private infix operator fun Instant.plus(seconds: Long): Date = Date.from(plusSeconds(seconds))
