package no.nav.aap.ktor.client.maskinporten.client

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.nimbusds.jwt.SignedJWT
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.http.ContentType.Application.FormUrlEncoded
import io.ktor.serialization.jackson.*
import java.time.Instant

const val GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
const val LEEWAY_SECONDS: Long = 20

interface Oauth2JwtProvider {
    suspend fun getToken(): String
}

class HttpClientMaskinportenTokenProvider(
    private val config: MaskinportenConfig,
) : Oauth2JwtProvider {
    private val grants: JwtGrantFactory = JwtGrantFactory(config)
    private val cache = mutableMapOf<String, Token>()
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            jackson {
                registerModule(JavaTimeModule())
                disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            }
        }
    }

    override suspend fun getToken(): String {
        val token = cache[config.scope]?.takeUnless(Token::hasExpired) ?: fetchToken()
        return token.access_token.let(SignedJWT::parse).parsedString
    }

    private suspend fun fetchToken(): Token =
        client.post(config.tokenEndpointUrl) {
            contentType(FormUrlEncoded)
            setBody("grant_type=$GRANT_TYPE&assertion=${grants.jwt}")
        }.body<Token>().also { token ->
            cache[config.scope] = token
        }

    private data class Token(val access_token: String) {
        private val signedJwt = SignedJWT.parse(access_token)
        private val expiry = signedJwt.jwtClaimsSet.expirationTime.toInstant().minusSeconds(LEEWAY_SECONDS)

        fun hasExpired() = expiry < Instant.now()
    }
}
