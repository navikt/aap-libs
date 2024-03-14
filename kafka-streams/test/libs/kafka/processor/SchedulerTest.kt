package libs.kafka.processor

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import libs.kafka.StreamsMock
import libs.kafka.Tables
import libs.kafka.Topics
import libs.kafka.processor.state.GaugeStoreEntriesStateScheduleProcessor
import libs.kafka.produce
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
