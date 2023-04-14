package no.nav.aap.kafka.streams.v2.processor

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.streams.v2.*
import no.nav.aap.kafka.streams.v2.processor.state.GaugeStoreEntriesStateScheduleProcessor
import no.nav.aap.kafka.streams.v2.processor.state.MigrateStateInitProcessor
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.streams.state.ValueAndTimestamp
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.Ignore
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

    @Test
    // TODO: fiks testen så vi får verifisert at den migrerer i processoren og ikke bare i deserializeren.
    @Ignore("fiks testen så vi får verifisert at den migrerer i processoren og ikke bare i deserializeren.")
    fun `migration scheduler`() {
        val producer = MockProducer(true, Topics.migrateable.keySerde.serializer(), Topics.migrateable.valueSerde.serializer())

        val kafka = StreamsMock.withTopology {
            val ktable = consume(Tables.E)
            ktable.init(
                MigrateStateInitProcessor(
                    producer = producer,
                    ktable = ktable,
                )
            )
        }

        val stateStore = kafka.getTimestampedKeyValueStore(Tables.E)
        stateStore.put("1", ValueAndTimestamp.make(VersionedString("E", 1), Instant.now().toEpochMilli()))
    }
}
