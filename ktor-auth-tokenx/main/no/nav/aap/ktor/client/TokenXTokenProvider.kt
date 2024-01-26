package no.nav.aap.ktor.client

import com.nimbusds.jose.JOSEObjectType
import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.JWSHeader
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jose.jwk.RSAKey
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.SignedJWT
import io.ktor.client.call.body
import io.ktor.client.request.accept
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.serialization.jackson.jackson
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*

private val secureLog = LoggerFactory.getLogger("secureLog")
class TokenXTokenProvider(
    private val config: TokenXProviderConfig,
    private val audience: String,
    private val client: io.ktor.client.HttpClient = defaultHttpClient,
) {

    private val jwtFactory = JwtGrantFactory(config)
    suspend fun getOnBehalfOfToken(tokenx_token: String) = getAccessToken(tokenx_token+audience) {
        """
            grant_type=urn:ietf:params:oauth:grant-type:token-exchange
            &client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer
            &subject_token_type=urn:ietf:params:oauth:token-type:jwt
            &client_assertion=${jwtFactory.jwt}
            &audience=$audience
            &subject_token=$tokenx_token
        """.trimIndent()
            .replace("\n", "")
    }

    private val cache = TokenCache()

    private suspend fun getAccessToken(cacheKey: String, body: () -> String): String {
        cache.logg(secureLog)
        val token = cache.get(cacheKey)
            ?: client.post(config.tokenEndpoint) {
                accept(io.ktor.http.ContentType.Application.Json)
                contentType(io.ktor.http.ContentType.Application.FormUrlEncoded)
                setBody(body())
            }.also {
                if (!it.status.isSuccess()) {
                    secureLog.warn("Feilet token-kall {}: {}", it.status.value, it.bodyAsText())
                }
            }.body<Token>().also {
                cache.add(cacheKey, it)
            }

        return token.access_token
    }

    private companion object {
        private val defaultHttpClient = io.ktor.client.HttpClient(io.ktor.client.engine.cio.CIO) {
            install(io.ktor.client.plugins.contentnegotiation.ContentNegotiation) {
                jackson {
                    registerModule(com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                    disable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                }
            }
        }
    }
}

internal class JwtGrantFactory(private val config: TokenXProviderConfig) {
    internal val jwt: String get() = signedJwt.serialize()

    private val privateKey = RSAKey.parse(config.privateKey)
    private val signedJwt get() = SignedJWT(jwsHeader, jwtClaimSet).apply { sign(RSASSASigner(privateKey)) }
    private val jwsHeader
        get() = JWSHeader.Builder(JWSAlgorithm.RS256)
            .keyID(privateKey.keyID)
            .type(JOSEObjectType.JWT)
            .build()

    private val jwtClaimSet: JWTClaimsSet
        get() = JWTClaimsSet.Builder().apply {
            subject(config.clientId)
            issuer(config.clientId)
            audience(config.tokenEndpoint)
            jwtID(UUID.randomUUID().toString())
            notBeforeTime(Date())
            issueTime(Date())
            expirationTime(Date.from(Instant.now().plusSeconds(120)))
        }.build()
}