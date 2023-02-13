package no.nav.aap.kafka.streams.v2.visual


import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyDescription.*

internal object Mermaid {

    internal fun generateCompleteGraph(
        topology: Topology,
        direction: Direction = Direction.LR,
    ): String = topology.describe()
        .subtopologies()
        .flatMap { it.nodes() }
        .let { createGraph(it, direction) }

    private fun createGraph(
        nodes: Collection<Node>,
        direction: Direction,
    ): String {
        val sources = nodes.filterIsInstance<Source>()
        val processors = nodes.filterIsInstance<Processor>()
        val sinks = nodes.filterIsInstance<Sink>()

        val statefulProcessors = processors.filter { it.stores().isNotEmpty() }
        val statefulToTableProcessors = statefulProcessors.filter { it.name().contains("-to-table") }
        val statefulJobs = statefulProcessors.filter { it.successors().isEmpty() }
        val statefulStores = statefulProcessors.flatMap(Processor::stores).distinct()
        val statefulJoins = statefulProcessors.filter { it.name().contains("-join-") }

        val branchedProcessors = processors.filter { it.successors().size > 1 }

        val topicNames = (sources + sinks).flatMap(::topicNames).distinct()
        val jobNames = statefulJobs.map(Processor::name).distinct()

        val topicStyle = topicNames.joinToString(EOL) { style(it, "#c233b4") }
        val storeStyle = statefulStores.joinToString(EOL) { style(it, "#78369f") }
        val jobStyle = jobNames.joinToString(EOL) { style(it, "#78369f") }

        return template(
            direction = direction,
            topics = topicNames.joinToString(EOL + TAB) { it.topicShape },
            joins = statefulJoins.joinToString(EOL + TAB) { it.name().joinShape },
            stores = statefulStores.joinToString(EOL + TAB) { it.storeShape },
            jobs = jobNames.joinToString(EOL + TAB) { it.jobShape },
            styles = listOf(topicStyle, storeStyle, jobStyle).joinToString(EOL),
            jobStreams = jobStreams(statefulJobs).joinToString(EOL + TAB),
            branchStreams = branchStreams(branchedProcessors, statefulJoins).joinToString(EOL + TAB),
            joinStreams = joinStreams(statefulJoins).joinToString(EOL + TAB),
            toTableStreams = toTableStreams(statefulToTableProcessors).joinToString(EOL + TAB),
            repartitionStreams = repartitionStreams(sources, sinks).joinToString(EOL + TAB),
        )
    }

    private fun toTableStreams(statefulToTableProcessors: List<Processor>): List<String> {
        return statefulToTableProcessors.flatMap { processor ->
            val sources = findSources(processor)
            sources.flatMap { source ->
                val sourceTopics = source.topicSet()
                sourceTopics.flatMap { sourceTopic ->
                    val stores = processor.stores()
                    stores.map { store ->
                        path(sourceTopic, store)
                    }
                }
            }
        }
    }

    private fun jobStreams(statefulProcessors: List<Processor>): List<String> =
        statefulProcessors.flatMap { processor ->
            processor.stores().map { storeName -> path(processor.name(), storeName) }
        }

    private fun branchStreams(branchedProcessors: List<Processor>, joins: List<Processor>): List<String> {
        return branchedProcessors
            .filterNot { isTraversingAJoin(it, joins) }
            .flatMap { processor ->
                findSources(processor).flatMap { source ->
                    source.topicSet().flatMap { sourceTopic ->
                        findSinks(processor).filterIsInstance<Sink>().map { it.topic() }.map { sinkTopic ->
                            path(
                                sourceTopic,
                                sinkTopic,
//                            processor.name(),
                            )
                        }
                    }
                }
            }.distinct()
    }

    private fun repartitionStreams(sources: List<Source>, sinks: List<Sink>): List<String> {
        val repartitionTopicNames = sinks
            .filter { it.topic().contains("-repartition") }
            .map(Sink::topic)
            .distinct()

        return sources
            .flatMap { findSinks(it).filterIsInstance<Sink>().map { sink -> it.topicSet() to sink.topic() } }
            .filter { (_, sink) -> sink in repartitionTopicNames }
            .flatMap { (sources, sink) -> sources.map { source -> path(source, sink, "rekey") } }
            .distinct()
    }

    private fun joinStreams(statefulJoins: List<Processor>): List<String> =
        statefulJoins.flatMap { stateJoin ->
            stateJoin.stores().flatMap { store ->
                val sinkTopics = findSinks(stateJoin).filterIsInstance<Sink>()
                findSources(stateJoin).flatMap { source ->
                    val leftSide = source.topicSet().map { sourceTopicName -> path(sourceTopicName, stateJoin.name()) }
                    val rightSide = listOf(path(store, stateJoin.name()))
                    val destination = sinkTopics.map { sinkTopic ->
                        path(
                            stateJoin.name(),
                            sinkTopic.topic(),
//                            sinkTopic.name(),
                        )
                    }
                    leftSide union rightSide union destination
                }
            }
        }.distinct()

    private val String.topicShape get() = "$this([$this])"
    private val String.joinShape get() = if (this.contains("-left-join-")) "$this{left-join}" else "$this{join}"
    private val String.storeShape get() = "$this[($this)]"
    private val String.jobShape get() = "$this(($this))"

    private fun path(from: String, to: String, label: String? = null): String = when {
        label != null -> "$from --> |$label| $to"
        else -> "$from --> $to"
    }

    private fun topicNames(node: Node) = when (node) {
        is Processor -> listOf(node.name())
        is Sink -> listOf(node.topic())
        is Source -> node.topicSet()
        else -> emptyList()
    }

    private fun style(name: String, hexColor: String) =
        "style $name fill:$hexColor, stroke:#2a204a, stroke-width:2px, color:#2a204a"

    private fun findSinks(node: Node): List<Node> =
        if (node.successors().isNotEmpty()) node.successors().flatMap { findSinks(it) }
        else listOf(node)

    private fun findSources(node: Node): List<Source> =
        if (node.predecessors().isNotEmpty()) node.predecessors().flatMap { findSources(it) }
        else if (node is Source) listOf(node)
        else error("source node was not of type TopologyDescription.Source")

    private fun isTraversingAJoin(node: Node, joins: List<Processor>): Boolean {
        return node.predecessors().any { predecessor ->
            joins.any { join -> join == predecessor }
        }
    }

    enum class Direction(private val description: String) {
        TB("top to bottom"),
        TD("top-down/ same as top to bottom"),
        BT("bottom to top"),
        RL("right to left"),
        LR("left to right");
    }

    private fun template(
        direction: Direction,
        topics: String,
        joins: String,
        stores: String,
        jobs: String,
        styles: String,
        joinStreams: String,
        branchStreams: String,
        toTableStreams: String,
        jobStreams: String,
        repartitionStreams: String,
    ) = """
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#07cff6', 'textColor': '#dad9e0', 'lineColor': '#07cff6'}}}%%

graph ${direction.name}

subgraph Topologi
    %% TOPICS
    $topics

    %% JOINS
    $joins

    %% STATE STORES
    $stores

    %% PROCESSOR API JOBS
    $jobs
    
    %% JOIN STREAMS
    $joinStreams

    %% TO TABLE STREAMS
    $toTableStreams

    %% JOB STREAMS
    $jobStreams
    
    %% BRANCH STREAMS
    $branchStreams

    %% REPARTITION STREAMS
    $repartitionStreams
end

%% COLORS
%% light    #dad9e0
%% purple   #78369f
%% pink     #c233b4
%% dark     #2a204a
%% blue     #07cff6

%% STYLES
$styles
"""
}

private const val EOL = "\n"
private const val TAB = "\t"
