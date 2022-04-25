package no.nav.aap.kafka.streams

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory

private val secureLog = LoggerFactory.getLogger("secureLog")

fun <K, V, VO, VR> KStream<K, V>.join(joined: Joined<K, V, VO>, table: KTable<K, VO>, joiner: (V, VO) -> VR) =
    join(table, joiner, joined)!!

fun <K, V, VO, VR> KStream<K, V>.leftJoin(joined: Joined<K, V, VO>, table: KTable<K, VO>, joiner: (V, VO) -> VR) =
    leftJoin(table, joiner, joined)!!

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @param valueJoiner: The resulting join record
 */
fun <V, R, VR> KStream<String, V>.join(
    keyMapper: (String, V) -> String,
    table: GlobalKTable<String, R>,
    valueJoiner: (V, R) -> VR
) = join(table, keyMapper, valueJoiner)!!

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @param valueJoiner: The resulting join record
 */
fun <V, R, VR> KStream<String, V>.leftJoin(
    keyMapper: (String, V) -> String,
    table: GlobalKTable<String, R>,
    valueJoiner: (V, R) -> VR
) = leftJoin(table, keyMapper, valueJoiner)!!

fun <K, V, KR> KStream<K, V>.selectKey(name: String, mapper: KeyValueMapper<in K, in V, out KR>) =
    selectKey(mapper, named(name))!!

fun <V> KStream<String, V>.produce(topic: Topic<V>, named: () -> String) =
    peek { key, value -> secureLog.info("produced [${topic.name}] K:$key V:$value") }
        .to(topic.name, topic.produced(named()))

fun <V> KStream<String, V>.produce(table: Table<V>): KTable<String, V> =
    peek { key, value -> secureLog.info("produced [${table.stateStoreName}] K:$key V:$value") }
        .toTable(named("${table.name}-as-table"), materialized(table.stateStoreName, table.source))

@Suppress("UNCHECKED_CAST")
fun <V> KStream<String, V?>.filterNotNull(named: () -> String): KStream<String, V> =
    filter({ _, value -> value != null }, named(named())) as KStream<String, V>

fun <K, V> KStream<K, V>.filter(predicate: Predicate<in K, in V>, named: () -> String): KStream<K, V> =
    filter(predicate, named(named()))

fun <K, V> KStream<K, V>.filter(named: String, predicate: (K, V) -> Boolean): KStream<K, V> =
    filter(predicate) { named }

fun <K, V, VR> KStream<K, V>.mapValues(named: Named, mapper: (V) -> VR): KStream<K, VR> =
    mapValues(mapper, named)

fun <K, V, VR> KStream<K, V>.flatMapValues(named: Named, mapper: (V) -> Iterable<VR>): KStream<K, VR> =
    flatMapValues(mapper, named)

/**
 * Await for the given store to be available
 */
fun <V> KStreams.waitForStore(name: String): ReadOnlyKeyValueStore<String, V> = runBlocking {
    secureLog.info("Waiting 10_000 ms for store $name to become available")
    val store = withTimeout(10_000L) {
        flow {
            while (true) {
                runCatching { getStore<V>(name) }
                    .getOrNull()?.let { emit(it) }
                delay(100)
            }
        }.firstOrNull()
    }

    store ?: error("state store not awailable after 10s")
}
