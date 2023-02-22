package no.nav.aap.ktor.config

import com.sksamuel.hoplite.ConfigLoader
import com.sksamuel.hoplite.yaml.YamlParser

inline fun <reified T : Any> loadConfig(resource: String = "/application.yml"): T =
    ConfigLoader.builder()
        .addFileExtensionMapping("yml", YamlParser())
        .build()
        .loadConfigOrThrow(resource)
