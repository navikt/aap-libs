package no.nav.aap.ktor.client.auth.util

internal fun String.asUrlPart() =
    this.trimIndent().replace("\n", "")

internal fun getEnvVar(envar: String) = System.getenv(envar) ?: error("missing envvar $envar")