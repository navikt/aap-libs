package no.nav.aap.ktor.config

import com.sksamuel.hoplite.ConfigLoader
import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.sources.MapPropertySource
import com.sksamuel.hoplite.yaml.YamlParser
import io.ktor.server.application.*
import io.ktor.server.config.*

inline fun <reified T : Any> Application.loadConfig(resource: String = "/application.yml"): T =
    ConfigLoader.builder()
        .addFileExtensionMapping("yml", YamlParser())
        .addKtorConfig(environment.config)
        .build()
        .loadConfigOrThrow(resource)

fun ConfigLoaderBuilder.addKtorConfig(config: ApplicationConfig) = apply {
    if (config is MapApplicationConfig) addPropertySource(MapPropertySource(config.toMap()))
}
