package no.nav.aap.ktor.client.auth.util

import com.nimbusds.jose.JOSEObjectType
import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.JWSHeader
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jose.jwk.RSAKey
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.SignedJWT

internal data class JwtConfig(
    val privateKey: String,
    val claimset: Map<String, Any>
)

internal class JwtGrantFactory(private val config: JwtConfig) {
    internal val jwt: String get() = signedJwt.serialize()

    private val privateKey = RSAKey.parse(config.privateKey)

    private val signedJwt get() = SignedJWT(jwsHeader, jwtClaimSet).apply {
        sign(RSASSASigner(privateKey))
    }

    private val jwsHeader
        get() = JWSHeader.Builder(JWSAlgorithm.RS256)
            .keyID(privateKey.keyID)
            .type(JOSEObjectType.JWT)
            .build()

    private val jwtClaimSet: JWTClaimsSet
        get() = JWTClaimsSet.Builder().apply {
            config.claimset.forEach { kv ->
                claim(kv.key, kv.value)
            }
        }.build()
}
