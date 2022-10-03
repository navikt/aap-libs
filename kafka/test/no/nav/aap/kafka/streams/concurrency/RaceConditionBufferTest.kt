package no.nav.aap.kafka.streams.concurrency

import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

internal class RaceConditionBufferTest {

    @Test
    fun `Buffrer og henter ut buffret versjon da den er nyest`() {
        val buffret = TestBufferable(2)
        val gammel = TestBufferable(1)

        val buffer = RaceConditionBuffer<String, TestBufferable>()

        buffer.lagre("", buffret)

        assertSame(buffret, buffer.velgNyeste("", gammel))
    }

    @Test
    fun `Buffrer og returnerer ny versjon`() {
        val ny = TestBufferable(2)
        val buffret = TestBufferable(1)

        val buffer = RaceConditionBuffer<String, TestBufferable>()

        buffer.lagre("", buffret)

        assertSame(ny, buffer.velgNyeste("", ny))
    }

    @Test
    fun `Sletter gamle buffrede verdier`() {
        val buffret = TestBufferable(2)
        val gammel = TestBufferable(1)

        val buffer = RaceConditionBuffer<String, TestBufferable>(0)

        buffer.lagre("", buffret)

        assertSame(gammel, buffer.velgNyeste("", gammel))
    }

    private class TestBufferable(private val sekvensnummer: Int) : Bufferable<TestBufferable> {
        override fun erNyere(other: TestBufferable): Boolean = this.sekvensnummer > other.sekvensnummer
        override fun toString(): String = "TestBufferable(sekvensnummer=$sekvensnummer)"
    }
}
