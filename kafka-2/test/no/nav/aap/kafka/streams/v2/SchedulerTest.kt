package no.nav.aap.kafka.streams.v2

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.streams.v2.processor.state.GaugeStoreEntriesStateScheduleProcessor
import no.nav.aap.kafka.streams.v2.processor.state.MigrateStateScheduleProcessor
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.time.DurationUnit
import kotlin.time.toDuration

internal class SchedulerTest {

    @Test
    fun `metric scheduler`() {
        val registry = SimpleMeterRegistry()

        val topology = topology {
            val table = consume(Topics.B).produce(Tables.B)

            table.schedule(
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
        kafka.advanceWallClockTime(2.toDuration(DurationUnit.MINUTES))
        assertEquals(1.0, gauge.value())
    }

    @Test
    fun `migration scheduler`() {
        val producer = MockProducer(true, Topics.E.keySerde.serializer(), Topics.E.valueSerde.serializer())

        val topology = topology {
            val ktable = consume(Topics.E).produce(Tables.E)
            ktable.schedule(
                MigrateStateScheduleProcessor(
                    named = "migrate",
                    interval = 2.toDuration(DurationUnit.MINUTES),
                    producer = producer,
                    ktable = ktable,
                    logValue = true
                )
            )
        }

        val kafka = kafka(topology)

        val stateStore = kafka.getTimestampedKeyValueStore(Tables.E)
        stateStore.put("1", ValueAndTimestamp.make(VersionedString("E", 1), Instant.now().toEpochMilli()))

        kafka.advanceWallClockTime(2.toDuration(DurationUnit.MINUTES))

        assertEquals(2, producer.history().last().value().version)

//        println(no.nav.aap.kafka.streams.v2.visual.PlantUML.generate(topology.build()))
    }
}
