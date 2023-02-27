package no.nav.aap.ktor.config

import io.ktor.server.config.*
import io.ktor.server.testing.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ConfigTest {

    data class TestConfig(val value: String)

    @Test
    fun test() {
        testApplication {
            environment {
                config = MapApplicationConfig("TEST_VALUE" to "hello")
            }
            application {
                val config = loadConfig<TestConfig>()
                assertEquals("hello", config.value)
            }
        }
    }
}
