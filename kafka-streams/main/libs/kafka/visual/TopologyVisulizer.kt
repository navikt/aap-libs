package libs.kafka.visual

import org.apache.kafka.streams.Topology

class TopologyVisulizer(
    private val topology: Topology,
) {
    fun uml() : String = PlantUML.generate(topology)

    fun mermaid(): Mermaid = Mermaid(topology)
}
