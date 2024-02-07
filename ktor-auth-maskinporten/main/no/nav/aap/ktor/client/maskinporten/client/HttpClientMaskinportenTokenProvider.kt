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

const val GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
const val LEEWAY_SECONDS: Long = 20

interface Oauth2JwtProvider {
    suspend fun getToken(): String
}

class HttpClientMaskinportenTokenProvider(
    private val config: MaskinportenConfig,
    private val client: HttpClient = defaultHttpClient
) : Oauth2JwtProvider {
    private val grants: JwtGrantFactory = JwtGrantFactory(config)
    private val cache = TokenCache()

    override suspend fun getToken(): String {
        val token = cache.get(config.scope)?.takeUnless(Token::hasExpired) ?: fetchToken()
        return token.access_token.let(SignedJWT::parse).parsedString
    }

    private suspend fun fetchToken(): Token =
        client.post(config.tokenEndpointUrl) {
            contentType(FormUrlEncoded)
            setBody("grant_type=$GRANT_TYPE&assertion=${grants.jwt}")
        }.body<Token>().also { token ->
            cache.add(config.scope, token)
        }

    private companion object {
        private val defaultHttpClient = HttpClient(CIO) {
            install(ContentNegotiation) {
                jackson {
                    registerModule(JavaTimeModule())
                    disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                }
            }
        }
    }
}
