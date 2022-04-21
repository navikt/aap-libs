package no.nav.aap.ktor.client

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.auth.*
import io.ktor.client.plugins.auth.providers.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.http.ContentType.Application.FormUrlEncoded
import io.ktor.serialization.jackson.*
import java.net.URL
import java.time.Instant

data class AzureConfig(val tokenEndpoint: URL, val clientId: String, val clientSecret: String)

/**
 * Install Auth client interceptor on a HttpClient.
 * Example usage:
 * ```
 *  HttpClient(CIO) { install(Auth) { azureAD(config, "some-scope") } }
 * ```
 */
class HttpClientAzureAdInterceptor(private val config: AzureConfig) {
    private val cache = mutableMapOf<String, Token>()
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) { jackson { registerModule(JavaTimeModule()) } }
    }

    internal suspend fun getToken(scope: String) = cache[scope]?.takeUnless(Token::isExpired) ?: fetchToken(scope)

    private suspend fun fetchToken(scope: String): Token =
        client.post(config.tokenEndpoint) {
            contentType(FormUrlEncoded)
            setBody("client_id=${config.clientId}&client_secret=${config.clientSecret}&scope=$scope&grant_type=client_credentials")
        }.body<Token>().also { token ->
            cache[scope] = token
        }

    companion object {
        private lateinit var interceptor: HttpClientAzureAdInterceptor

        fun Auth.azureAD(config: AzureConfig, scope: String) {
            interceptor = HttpClientAzureAdInterceptor(config)

            bearer {
                loadTokens {
                    BearerTokens(interceptor.getToken(scope).access_token, "")
                }
            }
        }
    }

    internal data class Token(val token_type: String, val expires_in: Long, val access_token: String) {
        private val expiresOn = Instant.now().plusSeconds(expires_in - 60)

        val isExpired get() = expiresOn < Instant.now()
    }
}
