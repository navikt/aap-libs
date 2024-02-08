package no.nav.aap.ktor.client.auth.util

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import org.slf4j.LoggerFactory

internal val defaultHttpClient = HttpClient(CIO) {
    install(ContentNegotiation) {
        jackson {
            registerModule(JavaTimeModule())
            disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        }
    }
}

internal class TokenClient(private val client: HttpClient) {
    private val cache = TokenCache()
    private val secureLog = LoggerFactory.getLogger("secureLog")

    suspend fun getAccessToken(tokenEndpoint: String, cacheKey: String, body: () -> String): String {
        val token = cache.get(cacheKey)
            ?: client.post(tokenEndpoint) {
                accept(ContentType.Application.Json)
                contentType(ContentType.Application.FormUrlEncoded)
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
}
