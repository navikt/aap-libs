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

const val GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
const val LEEWAY_SECONDS = 60

class HttpClientAdBehalfOfTokenProvider(
    private val config: AzureConfig,
    private val scope: String
) {
    private val cache = mutableMapOf<String, BehalfOfToken>()
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            jackson {
                registerModule(JavaTimeModule())
                disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            }
        }
    }

    suspend fun getBehalfOfToken(accessToken: String): String {
        val token = cache[scope]?.takeUnless(BehalfOfToken::hasExpired) ?: fetchToken(scope, accessToken)
        return token.access_token
    }

    private suspend fun fetchToken(scope: String, accessToken: String): BehalfOfToken =
        client.post(config.tokenEndpoint) {
            contentType(FormUrlEncoded)
            setBody("client_id=${config.clientId}&client_secret=${config.clientSecret}&assertion=$accessToken&scope=$scope&grant_type=$GRANT_TYPE&requested_token_use=on_behalf_of")
        }.body<BehalfOfToken>().also { token ->
            cache[scope] = token
        }

    private data class BehalfOfToken(
        val expires_in: Long,
        val access_token: String
    ) {
        private val expiry = Instant.now().plusSeconds(expires_in - LEEWAY_SECONDS)

        fun hasExpired() = expiry < Instant.now()
    }
}
