package no.nav.aap.ktor.client

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

class AzureAdTokenProvider(
    private val config: AzureConfig,
    private val scope: String,
    private val client: HttpClient = defaultHttpClient,
) {
    suspend fun getUsernamePasswordToken(username: String, password: String) = getAccessToken(username) {
        "client_id=${config.clientId}&scope=$scope&username=$username&password=$password&grant_type=password"
    }

    suspend fun getClientCredentialToken() = getAccessToken(scope) {
        "client_id=${config.clientId}&client_secret=${config.clientSecret}&scope=$scope&grant_type=client_credentials"
    }

    suspend fun getOnBehalfOfToken(accessToken: String) = getAccessToken(scope) {
        "client_id=${config.clientId}&client_secret=${config.clientSecret}&assertion=$accessToken&scope=$scope&grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&requested_token_use=on_behalf_of"
    }

    private val tokenCache = mutableMapOf<String, Token>()

    private suspend fun getAccessToken(cacheKey: String, body: () -> String): String {
//        val token = tokenCache[cacheKey]?.takeUnless(Token::hasExpired)
//            ?: client.post(config.tokenEndpoint) {
//                accept(ContentType.Application.Json)
//                contentType(ContentType.Application.FormUrlEncoded)
//                setBody(body())
//            }.body<Token>().also { fetchedToken ->
//                tokenCache[cacheKey] = fetchedToken
//            }
        val token = client.post(config.tokenEndpoint) {
            accept(ContentType.Application.Json)
            contentType(ContentType.Application.FormUrlEncoded)
            setBody(body())
        }
        println(token.contentType())
        return token.bodyAsText()
//        return token.access_token
    }

    companion object {
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
