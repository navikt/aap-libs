package no.nav.aap.kafka.streams.v2

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import no.nav.aap.kafka.serde.json.Migratable
import no.nav.aap.kafka.streams.v2.config.StreamsConfig
import no.nav.aap.kafka.streams.v2.serde.JsonSerde
import no.nav.aap.kafka.streams.v2.serde.StringSerde
import no.nav.aap.kafka.streams.v2.visual.TopologyVisulizer
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import kotlin.time.Duration
import kotlin.time.toJavaDuration

internal object Topics {
    val A = Topic("A", StringSerde)
    val B = Topic("B", StringSerde)
    val C = Topic("C", StringSerde)
    val D = Topic("D", StringSerde)
    val E = Topic("E", JsonSerde.jackson<VersionedString, VersionedString>(
        dtoVersion = 2,
        migrate = { prev -> prev.copy(version = 2) }
    ))
}

data class VersionedString(
    val value: String,
    val version: Int,
) : Migratable {
    private var erMigrertAkkuratNå: Boolean = false

    override fun markerSomMigrertAkkuratNå() {
        erMigrertAkkuratNå = true
    }

    override fun erMigrertAkkuratNå(): Boolean {
        return erMigrertAkkuratNå
    }
}

internal object Tables {
    val B = Table(Topics.B)
    val E = Table(Topics.E)
}

class KStreamsMock : KStreams {
    private lateinit var internalTopology: org.apache.kafka.streams.Topology
    private lateinit var internalStreams: TopologyTestDriver

    override fun connect(topology: Topology, config: StreamsConfig, registry: MeterRegistry) {
        topology.registerInternalTopology(this)
        internalStreams = TopologyTestDriver(internalTopology)
    }

    internal fun <V : Any> inputTopic(topic: Topic<V>): TestInputTopic<String, V> =
        internalStreams.createInputTopic(topic.name, topic.keySerde.serializer(), topic.valueSerde.serializer())

    internal fun <V : Any> outputTopic(topic: Topic<V>): TestOutputTopic<String, V> =
        internalStreams.createOutputTopic(topic.name, topic.keySerde.deserializer(), topic.valueSerde.deserializer())


    internal fun advanceWallClockTime(duration: Duration) =
        internalStreams.advanceWallClockTime(duration.toJavaDuration())

    internal fun <T : Any> getTimestampedKeyValueStore(table: Table<T>) =
        internalStreams.getTimestampedKeyValueStore<String, T>(table.stateStoreName)

    override fun ready(): Boolean = true
    override fun live(): Boolean = true
    override fun visulize(): TopologyVisulizer = TopologyVisulizer(internalTopology)
    override fun registerInternalTopology(internalTopology: org.apache.kafka.streams.Topology) {
        this.internalTopology = internalTopology
    }

    override fun <T : Any> getStore(table: Table<T>): StateStore<T> =
        StateStore(internalStreams.getKeyValueStore(table.stateStoreName))

    override fun close() = internalStreams.close()
}

internal fun <V> TestInputTopic<String, V>.produce(key: String, value: V): TestInputTopic<String, V> =
    pipeInput(key, value).let { this }

internal fun <V> TestInputTopic<String, V>.produceTombstone(key: String): TestInputTopic<String, V> =
    pipeInput(key, null).let { this }

internal fun kafka(topology: Topology): KStreamsMock = KStreamsMock().apply {
    connect(topology, StreamsConfig("", ""), SimpleMeterRegistry())
}
