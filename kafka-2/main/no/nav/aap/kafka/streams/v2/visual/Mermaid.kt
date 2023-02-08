package no.nav.aap.kafka.streams.v2.visual

//
//import no.nav.aap.kafka.streams.v2.Topology
//import org.apache.kafka.streams.TopologyDescription.*
//
//object Mermaid {
//    fun generate(
//        appName: String,
//        topology: Topology,
//        direction: Direction = Direction.LR,
//    ): String {
//        val nodes = topology.build().describe().subtopologies().flatMap(Subtopology::nodes)
//        val sources = nodes.filterIsInstance<Source>()
//        val processors = nodes.filterIsInstance<Processor>()
//        val sinks = nodes.filterIsInstance<Sink>()
//
//        val statefulProcessors = processors.filter { it.stores().isNotEmpty() }
//        val statefulJobs = statefulProcessors.filter { it.successors().isEmpty() }
//        val statefulStores = statefulProcessors.flatMap(Processor::stores).distinct()
//        val statefulJoins = statefulProcessors.filter { it.name().contains("joined") }
//
//        var joinIdSuffix = 0
//        val stateJoinsWithJoinId = statefulJoins.associateWith { "join-${joinIdSuffix++}" }
//
//        val topicNames = (sources + sinks).flatMap(::topicNames).distinct()
//        val jobNames = statefulJobs.map(Processor::name).distinct()
//
//        val topicStyle = topicNames.joinToString(EOL) { style(it, "#c233b4") }
//        val storeStyle = statefulStores.joinToString(EOL) { style(it, "#78369f") }
//        val jobStyle = jobNames.joinToString(EOL) { style(it, "#78369f") }
//
//        return template(
//            appName = appName,
//            direction = direction,
//            topics = topicNames.joinToString(EOL + TAB) { it.topicShape },
//            joins = stateJoinsWithJoinId.values.joinToString(EOL + TAB) { it.joinShape },
//            stores = statefulStores.joinToString(EOL + TAB) { it.storeShape },
//            jobs = jobNames.joinToString(EOL + TAB) { it.jobShape },
//            styles = listOf(topicStyle, storeStyle, jobStyle).joinToString(EOL),
//            jobStreams = jobStreams(statefulJobs).joinToString(EOL + TAB),
//            joinStreams = joinStreams(stateJoinsWithJoinId).joinToString(EOL + TAB),
//            repartitionStreams = repartitionStreams(sources, sinks).joinToString(EOL + TAB),
//        )
//    }
//
//    private fun jobStreams(statefulProcessors: List<Processor>): List<String> =
//        statefulProcessors.flatMap { processor ->
//            processor.stores().map { storeName -> path(processor.name(), storeName) }
//        }
//
//    private fun repartitionStreams(sources: List<Source>, sinks: List<Sink>): List<String> {
//        val repartitionTopicNames = sinks
//            .filter { it.topic().contains("-repartition") }
//            .map(Sink::topic)
//            .distinct()
//
//        return sources
//            .flatMap { findSinks(it).filterIsInstance<Sink>().map { sink -> it.topicSet() to sink.topic() } }
//            .filter { (_, sink) -> sink in repartitionTopicNames }
//            .flatMap { (sources, sink) -> sources.map { source -> path(source, sink, "re-key") } }
//            .distinct()
//    }
//
//    private fun joinStreams(stateJoinsWithJoinId: Map<Processor, String>): List<String> =
//        stateJoinsWithJoinId.flatMap { (stateJoin, joinId) ->
//            stateJoin.stores().flatMap { store ->
//                val sinkTopics = findSinks(stateJoin).filterIsInstance<Sink>()
//                findSources(stateJoin).flatMap { source ->
//                    val leftSide = source.topicSet().map { sourceTopicName -> path(sourceTopicName, joinId) }
//                    val rightSide = listOf(path(store, joinId))
//                    val destination = sinkTopics.map { sinkTopic -> path(joinId, sinkTopic.topic(), sinkTopic.name()) }
//                    leftSide union rightSide union destination
//                }
//            }
//        }.distinct()
//
//    private val String.topicShape get() = "$this([$this])"
//    private val String.joinShape get() = "$this{join}"
//    private val String.storeShape get() = "$this[($this)]"
//    private val String.jobShape get() = "$this(($this))"
//
//    private fun path(from: String, to: String, label: String? = null): String = when {
//        label != null -> "$from --> |$label| $to"
//        else -> "$from --> $to"
//    }
//
//    private fun topicNames(node: Node) = when (node) {
//        is Processor -> listOf(node.name())
//        is Sink -> listOf(node.topic())
//        is Source -> node.topicSet()
//        else -> emptyList()
//    }
//
//    private fun style(name: String, hexColor: String) =
//        "style $name fill:$hexColor, stroke:#2a204a, stroke-width:2px, color:#2a204a"
//
//    private fun findSinks(node: Node): List<Node> =
//        if (node.successors().isNotEmpty()) node.successors().flatMap { findSinks(it) }
//        else listOf(node)
//
//    private fun findSources(node: Node): List<Source> =
//        if (node.predecessors().isNotEmpty()) node.predecessors().flatMap { findSources(it) }
//        else if (node is Source) listOf(node)
//        else error("source node was not of type TopologyDescription.Source")
//
//    enum class Direction(private val description: String) {
//        TB("top to bottom"),
//        TD("top-down/ same as top to bottom"),
//        BT("bottom to top"),
//        RL("right to left"),
//        LR("left to right");
//    }
//
//    private fun template(
//        appName: String,
//        direction: Direction,
//        topics: String,
//        joins: String,
//        stores: String,
//        jobs: String,
//        styles: String,
//        joinStreams: String,
//        jobStreams: String,
//        repartitionStreams: String,
//    ) = """
//%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#07cff6', 'textColor': '#dad9e0', 'lineColor': '#07cff6'}}}%%
//
//graph ${direction.name}
//
//subgraph $appName
//    %% TOPICS
//    $topics
//
//    %% JOINS
//    $joins
//
//    %% STATE STORES
//    $stores
//
//    %% PROCESSOR API JOBS
//    $jobs
//
//    %% JOIN STREAMS
//    $joinStreams
//
//    %% JOB STREAMS
//    $jobStreams
//
//    %% REPARTITION STREAMS
//    $repartitionStreams
//end
//
//%% COLORS
//%% light    #dad9e0
//%% purple   #78369f
//%% pink     #c233b4
//%% dark     #2a204a
//%% blue     #07cff6
//
//%% STYLES
//$styles
//"""
//}
//
//private const val EOL = "\n"
//private const val TAB = "\t"
