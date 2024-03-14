package no.nav.aap.kafka.streams.v2.processor

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.streams.v2.StreamsMock
import no.nav.aap.kafka.streams.v2.Tables
import no.nav.aap.kafka.streams.v2.Topics
import no.nav.aap.kafka.streams.v2.processor.state.GaugeStoreEntriesStateScheduleProcessor
import no.nav.aap.kafka.streams.v2.produce
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.time.DurationUnit
import kotlin.time.toDuration

internal class SchedulerTest {
    @Test
    fun `metric scheduler`() {
        val registry = SimpleMeterRegistry()

        val kafka = StreamsMock.withTopology {
            val table = consume(Tables.B)

            table.schedule(
                GaugeStoreEntriesStateScheduleProcessor(
                    ktable = table,
                    interval = 2.toDuration(DurationUnit.MINUTES),
                    registry = registry,
                )
            )
        }

        kafka.inputTopic(Topics.B).produce("1", "B")

        val gauge = registry.get("kafka_stream_state_store_entries").gauge()
        assertEquals(0.0, gauge.value())

        // øker den interne kafka klokka for å simulere at det har gått 2 minutter, slik at scheduleren intreffer første runde.
        kafka.advanceWallClockTime(2.toDuration(DurationUnit.MINUTES))
        assertEquals(1.0, gauge.value())
    }
}
