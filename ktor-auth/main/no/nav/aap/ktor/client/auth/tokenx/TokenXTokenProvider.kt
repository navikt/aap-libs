package no.nav.aap.ktor.client.auth.tokenx

import io.ktor.client.*
import no.nav.aap.ktor.client.auth.util.TokenClient
import no.nav.aap.ktor.client.auth.util.JwtGrantFactory
import no.nav.aap.ktor.client.auth.util.asUrlPart
import no.nav.aap.ktor.client.auth.util.defaultHttpClient

class TokenXTokenProvider(
    private val config: TokenXConfig = TokenXConfig(),
    client: HttpClient = defaultHttpClient,
) {
    private val grants = JwtGrantFactory(config.toJwtConfig())
    private val tokenClient = TokenClient(client)

    suspend fun getOnBehalfOfToken(audience: String, token: String) =
        tokenClient.getAccessToken(config.tokenEndpoint, token + audience) {
            """
                grant_type=urn:ietf:params:oauth:grant-type:token-exchange&
                client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer&
                subject_token_type=urn:ietf:params:oauth:token-type:jwt&
                client_assertion=${grants.jwt}&
                audience=$audience&
                subject_token=$token
            """.asUrlPart()
        }
}
