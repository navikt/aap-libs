package no.nav.aap.ktor.client

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.http.ContentType.Application.FormUrlEncoded
import io.ktor.serialization.jackson.*
import java.time.Instant

const val GRANT_TYPE = "client_credentials"
const val LEEWAY_SECONDS = 60

class HttpClientAzureAdTokenProvider(
    private val config: AzureConfig,
    private val scope: String
) {
    private val cache = mutableMapOf<String, Token>()
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            jackson {
                registerModule(JavaTimeModule())
                disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            }
        }
    }

    suspend fun getToken(): String {
        val token = cache[scope]?.takeUnless(Token::hasExpired) ?: fetchToken(scope)
        return token.access_token
    }

    private suspend fun fetchToken(scope: String): Token =
        client.post(config.tokenEndpoint) {
            contentType(FormUrlEncoded)
            setBody("client_id=${config.clientId}&client_secret=${config.clientSecret}&scope=$scope&grant_type=$GRANT_TYPE")
        }.body<Token>().also { token ->
            cache[scope] = token
        }

    private data class Token(
        val expires_in: Long,
        val access_token: String
    ) {
        private val expiry = Instant.now().plusSeconds(expires_in - LEEWAY_SECONDS)

        fun hasExpired() = expiry < Instant.now()
    }
}
