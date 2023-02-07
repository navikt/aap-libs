package no.nav.aap.kafka.streams.v2

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.streams.v2.processor.state.GaugeStoreEntriesStateScheduleProcessor
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toJavaDuration

internal class SchedulerTest {

    @Test
    fun `metric scheduler`() {
        val registry = SimpleMeterRegistry()

        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)

            schedule(
                GaugeStoreEntriesStateScheduleProcessor(
                    named = "metrikker",
                    table = table,
                    interval = 2.toDuration(DurationUnit.MINUTES),
                    registry = registry,
                )
            )
        }

        val kafka = kafka(topology)

        kafka.inputTopic(Topics.B).produce("1", "B")

        val gauge = registry.get("kafka_stream_state_store_entries").gauge()
        assertEquals(0.0, gauge.value())

        // øker den interne kafka klokka for å simulere at det har gått 2 minutter, slik at scheduleren intreffer første runde.
        kafka.advanceWallClockTime(2.toDuration(DurationUnit.MINUTES).toJavaDuration())
        assertEquals(1.0, gauge.value())
    }
}
