package no.nav.aap.ktor.client

import java.net.URL

data class AzureConfig(
    val tokenEndpoint: URL, val clientId: String,
    val clientSecret: String
)
