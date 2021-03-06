package no.nav.aap.kafka.streams.extension

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import no.nav.aap.kafka.streams.*
import no.nav.aap.kafka.streams.transformer.LogConsumeTopic
import no.nav.aap.kafka.streams.transformer.LogProduceTable
import no.nav.aap.kafka.streams.transformer.LogProduceTopic
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.slf4j.LoggerFactory

private val secureLog = LoggerFactory.getLogger("secureLog")

/**
 * Inner join a KStream (left side) with a KTable (right side)
 *
 * Default timestamp extractor is [FailOnInvalidTimestamp][org.apache.kafka.streams.processor.FailOnInvalidTimestamp]
 * Fails when a [timestamp][org.apache.kafka.clients.consumer.ConsumerRecord] decreases
 *
 * @param K key
 * @param L left value
 * @param R right value
 * @param LR joined value
 * @receiver Left side of the join
 * @param joined: Key serde and value serdes used to deserialize both sides of the join
 * @param table: Right side of the join
 * @param joiner: Function to perform the join with the object
 */
fun <K, L, R, LR> KStream<K, L>.join(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
    joiner: (L, R) -> LR,
) = join(table, joiner, joined)!!

/**
 * Left join a KStream (left side) with a KTable (optional right side)
 *
 * Default timestamp extractor is [FailOnInvalidTimestamp][org.apache.kafka.streams.processor.FailOnInvalidTimestamp]
 * Fails when a [timestamp][org.apache.kafka.clients.consumer.ConsumerRecord] decreases
 *
 * @param K key.
 * @param L left value
 * @param R right value
 * @param LR joined value
 * @receiver Left side of the join
 * @param joined: Key serde and value serdes used to deserialize both sides of the join
 * @param table: Right side of the join
 * @param joiner: Function to perform the join with the object
 */
fun <K, L, R, LR> KStream<K, L>.leftJoin(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
    joiner: (L, R?) -> LR,
) = leftJoin(table, joiner, joined)!!

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @param valueJoiner: The resulting join record
 */
fun <K, L, R, LR> KStream<K, L>.join(
    keyMapper: (K, L) -> K,
    table: GlobalKTable<K, R>,
    valueJoiner: (L, R) -> LR
) = join(table, keyMapper, valueJoiner)!!

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @param valueJoiner: The resulting join record
 */
fun <K, L, R, LR> KStream<K, L>.leftJoin(
    keyMapper: (K, L) -> K,
    table: GlobalKTable<K, R>,
    valueJoiner: (L, R?) -> LR
) = leftJoin(table, keyMapper, valueJoiner)!!

fun <K, V, KR> KStream<K, V>.selectKey(
    name: String,
    mapper: KeyValueMapper<in K, in V, out KR>,
) = selectKey(mapper, named(name))!!

/**
 * @param logValue Logs record values to secure-logs when true
 */
fun <V> KStream<String, V>.produce(topic: Topic<V>, name: String, logValue: Boolean = false) = this
    .logProduced(topic, named("log-$name"), logValue)
    .to(topic.name, topic.produced(name))

/**
 * Produser records inkludert tombstones til en ktable
 * @param logValue Logs record values to secure-logs when true
 */
fun <V> KStream<String, V?>.produce(table: Table<V>, logValue: Boolean = false): KTable<String, V> = this
    .logProduced(table, named("log-produced-${table.name}"), logValue)
    .toTable(named("${table.name}-as-table"), materialized(table.stateStoreName, table.source))
    .filterNotNull("filter-not-null-${table.name}-as-table")

/**
 * @param logValue Logs record values to secure-logs when true
 */
internal fun <K, V> KStream<K, V?>.logConsumed(
    topic: Topic<V>,
    logValue: Boolean = false,
): KStream<K, V?> = transformValues(
    ValueTransformerWithKeySupplier { LogConsumeTopic("Konsumerer Topic", logValue) },
    named("log-consume-${topic.name}")
)

/**
 * @param logValue Logs record values to secure-logs when true
 */
internal fun <K, V> KStream<K, V>.logProduced(
    topic: Topic<V>,
    named: Named,
    logValue: Boolean = false,
): KStream<K, V> = transformValues(
    ValueTransformerWithKeySupplier { LogProduceTopic("Produserer til Topic", topic, logValue) },
    named
)

/**
 * @param logValue Logs record values to secure-logs when true
 */
internal fun <K, V> KStream<K, V?>.logProduced(
    table: Table<V>,
    named: Named,
    logValue: Boolean = false,
): KStream<K, V> = transformValues(
    ValueTransformerWithKeySupplier { LogProduceTable<K, V>("Produserer til KTable", table, logValue) },
    named
)

@Suppress("UNCHECKED_CAST")
fun <K, V> KTable<K, V?>.filterNotNull(name: String): KTable<K, V> =
    filter({ _, value -> value != null }, named(name)) as KTable<K, V>

@Suppress("UNCHECKED_CAST")
fun <K, V> KStream<K, V?>.filterNotNull(name: String): KStream<K, V> =
    filter({ _, value -> value != null }, named(name)) as KStream<K, V>

fun <K, V> KStream<K, V>.filter(name: String, predicate: (K, V) -> Boolean): KStream<K, V> =
    filter(predicate, named(name))

fun <K, V, VR> KStream<K, V>.mapValues(name: String, mapper: (V) -> VR): KStream<K, VR> =
    mapValues(mapper, named(name))

fun <K, V, VR> KStream<K, V>.mapValues(name: String, mapper: (K, V) -> VR): KStream<K, VR> =
    mapValues(mapper, named(name))

fun <K, V, VR> KStream<K, V>.flatMapValues(name: String, mapper: (V) -> Iterable<VR>): KStream<K, VR> =
    flatMapValues(mapper, named(name))

fun <K, V, VR> KStream<K, V>.flatMapValues(name: String, mapper: (K, V) -> Iterable<VR>): KStream<K, VR> =
    flatMapValues(mapper, named(name))

fun <K, V, VR> KStream<K, V?>.mapNotNull(name: String, mapper: (V) -> VR?): KStream<K, VR> = this
    .filterNotNull("$name-filter-premap")
    .mapValues("$name-map", mapper)
    .filterNotNull("$name-filter-postmap")

fun <K, V, VR> KStream<K, V?>.mapNotNull(name: String, mapper: (K, V) -> VR?): KStream<K, VR> = this
    .filterNotNull("$name-filter-premap")
    .mapValues("$name-map", mapper)
    .filterNotNull("$name-filter-postmap")

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
