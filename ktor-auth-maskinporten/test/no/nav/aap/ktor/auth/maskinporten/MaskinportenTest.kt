package no.nav.aap.ktor.auth.maskinporten

import com.nimbusds.jose.jwk.RSAKey
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.auth.*
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.testing.*
import no.nav.aap.ktor.auth.maskinporten.client.MaskinportenConfig
import no.nav.aap.ktor.auth.maskinporten.ktor.MaskinportenAuthProvider
import org.junit.Ignore
import org.junit.jupiter.api.Test

//class MaskinportenTest {

//    @Test
//    @Ignore
//    fun `maskinporten auth provider will add maskinporten token to requests`() {
//        val config = MaskinportenConfig(
//            tokenEndpointUrl = "MASKINPORTEN_TOKEN_ENDPOINT",
//            clientId = "MASKINPORTEN_CLIENT_ID",
//            privateKey = RSAKey.parse("MASKINPORTEN_CLIENT_JWK"),
//            scope = "MASKINPORTEN_SCOPES",
//            resource = "https://aap-api.dev.intern.nav.no", // hvordan appen identifiseres av issuer, f.eks ingress eller clientId
//            issuer = "MASKINPORTEN_ISSUER",
//            expireAfterSec = 120,
//        )
//
//        testApplication {
//            application {
//                val eksternTjeneste = HttpClient(CIO) {
//                    install(Auth) {
//                        val provider = MaskinportenAuthProvider(config)
//                        providers.add(provider)
//                    }
//                }
//            }
//        }
//
//    }

//}

//internal fun Application.maskinportenMock(config: MaskinportenConfig) {
    /**
    maskinporten:
        enabled: true
        scopes:
        consumes:
            - name: "nav:aap:vedtak"
            - name: "nav:aap:meldepliktshendelser"
     */
//    install(ContentNegotiation) { jackson {} }
//    routing {
//        post(config.tokenEndpointUrl) {
//            call.respond(HttpStatusCode.OK, RSAKey.parse("a.b.c"))
//        }
//    }
//}
