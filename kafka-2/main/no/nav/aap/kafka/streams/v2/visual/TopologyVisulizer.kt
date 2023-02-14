package no.nav.aap.kafka.streams.v2.visual

import org.apache.kafka.streams.Topology

class TopologyVisulizer(
    val topology: Topology,
) {
    fun uml() : String = PlantUML.generate(topology)

    fun mermaid(): Mermaid = Mermaid(topology)
}
