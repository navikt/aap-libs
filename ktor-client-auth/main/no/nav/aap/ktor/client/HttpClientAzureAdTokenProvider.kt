package no.nav.aap.ktor.client

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.http.ContentType.Application.FormUrlEncoded
import io.ktor.serialization.jackson.*
import java.net.URL
import java.time.Instant

data class AzureConfig(val tokenEndpoint: URL, val clientId: String, val clientSecret: String)

class HttpClientAzureAdTokenProvider(
    private val config: AzureConfig,
    private val scope: String
) {
    private val cache = mutableMapOf<String, Token>()
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) { jackson {
            registerModule(JavaTimeModule()) }

        }
    }

    suspend fun getToken() = (cache[scope]?.takeUnless(Token::isExpired) ?: fetchToken(scope)).access_token

    private suspend fun fetchToken(scope: String): Token =
        client.post(config.tokenEndpoint) {
            contentType(FormUrlEncoded)
            setBody("client_id=${config.clientId}&client_secret=${config.clientSecret}&scope=$scope&grant_type=client_credentials")
        }.body<Token>().also { token ->
            cache[scope] = token
        }

    private data class Token(val token_type: String, val expires_in: Long, val ext_expires_in: Long, val access_token: String) {
        private val expiresOn = Instant.now().plusSeconds(expires_in - 60)

        val isExpired get() = expiresOn < Instant.now()
    }
}
