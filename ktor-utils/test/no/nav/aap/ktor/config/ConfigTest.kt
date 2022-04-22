package no.nav.aap.ktor.config

import org.junit.Test
import kotlin.test.assertEquals

class ConfigTest {

    data class TestConfig(val value: String)

    @Test
    fun test() {
        val config = loadConfig<TestConfig>()
        assertEquals("hello", config.value)
    }
}