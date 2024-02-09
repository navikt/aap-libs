package no.nav.aap.ktor.client.auth.azure

import no.nav.aap.ktor.client.auth.util.getEnvVar

data class AzureConfig(
    val tokenEndpoint: String = getEnvVar("AZURE_OPENID_CONFIG_TOKEN_ENDPOINT"),
    val clientId: String = getEnvVar("AZURE_APP_CLIENT_ID"),
    val clientSecret: String = getEnvVar("AZURE_APP_CLIENT_SECRET"),
    val jwksUri: String = getEnvVar("AZURE_OPENID_CONFIG_JWKS_URI"),
    val issuer: String = getEnvVar("AZURE_OPENID_CONFIG_ISSUER")
)
