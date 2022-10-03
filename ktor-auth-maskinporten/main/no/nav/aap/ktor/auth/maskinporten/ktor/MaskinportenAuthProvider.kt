package no.nav.aap.ktor.auth.maskinporten.ktor

import io.ktor.client.plugins.auth.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.http.auth.*
import no.nav.aap.ktor.auth.maskinporten.client.HttpClientMaskinportenTokenProvider
import no.nav.aap.ktor.auth.maskinporten.client.MaskinportenConfig

class MaskinportenAuthProvider(
    config: MaskinportenConfig,
) : AuthProvider {
    private val tokenClient = HttpClientMaskinportenTokenProvider(config)

    @Suppress("OVERRIDE_DEPRECATION")
    override val sendWithoutRequest: Boolean = error("Please use sendWithoutRequest function instead")

    override fun sendWithoutRequest(request: HttpRequestBuilder) = true
    override fun isApplicable(auth: HttpAuthHeader): Boolean = true

    override suspend fun addRequestHeaders(request: HttpRequestBuilder, authHeader: HttpAuthHeader?) {
        request.header(HttpHeaders.Authorization, "Bearer ${tokenClient.getToken()}")
    }
}
