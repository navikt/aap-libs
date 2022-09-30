package no.nav.aap.kafka.streams.extension

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import no.nav.aap.kafka.streams.*
import no.nav.aap.kafka.streams.concurrency.Bufferable
import no.nav.aap.kafka.streams.concurrency.RaceConditionBuffer
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
 * @receiver Left side of the join
 * @param joined: Key serde and value serdes used to deserialize both sides of the join
 * @param table: Right side of the join
 * @return A stream with left and right values joined in a Pair
 */
fun <K, L, R> KStream<K, L>.join(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
): KStream<K, Pair<L, R>> = join(joined, table, ::Pair)

/**
 * Inner join a KStream (left side) with a KTable (right side)
 *
 * Default timestamp extractor is [FailOnInvalidTimestamp][org.apache.kafka.streams.processor.FailOnInvalidTimestamp]
 * Fails when a [timestamp][org.apache.kafka.clients.consumer.ConsumerRecord] decreases
 *
 * @param K key
 * @param L left value
 * @param R right value
 * @receiver Left side of the join
 * @param joined: Key serde and value serdes used to deserialize both sides of the join
 * @param table: Right side of the join
 * @param buffer: When race condition is a problem, buffer values in memory in between streams
 * @return A stream with left and right values joined in a Pair
 */
fun <K, L, R : Bufferable<R>> KStream<K, L>.join(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
    buffer: RaceConditionBuffer<K, R>,
): KStream<K, Pair<L, R>> = join(joined, table, buffer, ::Pair)

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
): KStream<K, LR> = join(table, joiner, joined)

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
 * @param buffer: When race condition is a problem, buffer values in memory in between streams
 * @param joiner: Function to perform the join with the object
 */
fun <K, L, R : Bufferable<R>, LR> KStream<K, L>.join(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
    buffer: RaceConditionBuffer<K, R>,
    joiner: (L, R) -> LR,
): KStream<K, LR> =
    join(
        table,
        { key, left, right ->
            val rightOrBuffered = buffer.velgNyeste(key, right)
            joiner(left, rightOrBuffered)
        },
        joined
    )

/**
 * Inner join a KStream (left side) with a KTable (right side)
 *
 * Default timestamp extractor is [FailOnInvalidTimestamp][org.apache.kafka.streams.processor.FailOnInvalidTimestamp]
 * Fails when a [timestamp][org.apache.kafka.clients.consumer.ConsumerRecord] decreases
 *
 * @param K key
 * @param L left value
 * @param R right value
 * @param KLR joined value
 * @receiver Left side of the join
 * @param joined: Key serde and value serdes used to deserialize both sides of the join
 * @param table: Right side of the join
 * @param joiner: Function to perform the join with the object
 */
fun <K, L, R, KLR> KStream<K, L>.join(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
    joiner: (K, L, R) -> KLR,
): KStream<K, KLR> = join(table, joiner, joined)

/**
 * Left join a KStream (left side) with a KTable (optional right side)
 *
 * Default timestamp extractor is [FailOnInvalidTimestamp][org.apache.kafka.streams.processor.FailOnInvalidTimestamp]
 * Fails when a [timestamp][org.apache.kafka.clients.consumer.ConsumerRecord] decreases
 *
 * @param K key.
 * @param L left value
 * @param R right value
 * @receiver Left side of the join
 * @param joined: Key serde and value serdes used to deserialize both sides of the join
 * @param table: Right side of the join
 * @return A stream with left and right values joined in a Pair
 */
fun <K, L, R> KStream<K, L>.leftJoin(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
): KStream<K, Pair<L, R?>> = leftJoin(joined, table, ::Pair)

/**
 * Left join a KStream (left side) with a KTable (optional right side)
 *
 * Default timestamp extractor is [FailOnInvalidTimestamp][org.apache.kafka.streams.processor.FailOnInvalidTimestamp]
 * Fails when a [timestamp][org.apache.kafka.clients.consumer.ConsumerRecord] decreases
 *
 * @param K key.
 * @param L left value
 * @param R right value
 * @receiver Left side of the join
 * @param joined: Key serde and value serdes used to deserialize both sides of the join
 * @param table: Right side of the join
 * @param buffer: When race condition is a problem, buffer values in memory in between streams
 * @return A stream with left and right values joined in a Pair
 */
fun <K, L, R : Bufferable<R>> KStream<K, L>.leftJoin(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
    buffer: RaceConditionBuffer<K, R>
): KStream<K, Pair<L, R?>> = leftJoin(joined, table, buffer, ::Pair)

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
): KStream<K, LR> = leftJoin(table, joiner, joined)

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
 * @param buffer: When race condition is a problem, buffer values in memory in between streams
 * @param joiner: Function to perform the join with the object
 */
fun <K, L, R : Bufferable<R>, LR> KStream<K, L>.leftJoin(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
    buffer: RaceConditionBuffer<K, R>,
    joiner: (L, R?) -> LR,
): KStream<K, LR> =
    leftJoin(
        table,
        { key, left, right ->
            val bufferedOrRight = right?.let { buffer.velgNyeste(key, it) }
            joiner(left, bufferedOrRight)
        },
        joined
    )

/**
 * Left join a KStream (left side) with a KTable (optional right side)
 *
 * Default timestamp extractor is [FailOnInvalidTimestamp][org.apache.kafka.streams.processor.FailOnInvalidTimestamp]
 * Fails when a [timestamp][org.apache.kafka.clients.consumer.ConsumerRecord] decreases
 *
 * @param K key.
 * @param L left value
 * @param R right value
 * @param KLR joined value
 * @receiver Left side of the join
 * @param joined: Key serde and value serdes used to deserialize both sides of the join
 * @param table: Right side of the join
 * @param joiner: Function to perform the join with the object
 */
fun <K, L, R, KLR> KStream<K, L>.leftJoin(
    joined: Joined<K, L, R>,
    table: KTable<K, R>,
    joiner: (K, L, R?) -> KLR,
): KStream<K, KLR> = leftJoin(table, joiner, joined)

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @return A stream with left and right values joined in a Pair
 */
fun <K, L, R> KStream<K, L>.join(
    keyMapper: (K, L) -> K,
    table: GlobalKTable<K, R>,
): KStream<K, Pair<L, R>> = join(keyMapper, table, ::Pair)

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @param valueJoiner: The resulting join record
 */
fun <K, L, R, LR> KStream<K, L>.join(
    keyMapper: (K, L) -> K,
    table: GlobalKTable<K, R>,
    valueJoiner: (L, R) -> LR
): KStream<K, LR> = join(table, keyMapper, valueJoiner)

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @param valueJoiner: The resulting join record
 */
fun <K, L, R, KLR> KStream<K, L>.join(
    keyMapper: (K, L) -> K,
    table: GlobalKTable<K, R>,
    valueJoiner: (K, L, R) -> KLR
): KStream<K, KLR> = join(table, keyMapper, valueJoiner)

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @return A stream with left and right values joined in a Pair
 */
fun <K, L, R> KStream<K, L>.leftJoin(
    keyMapper: (K, L) -> K,
    table: GlobalKTable<K, R>,
): KStream<K, Pair<L, R?>> = leftJoin(keyMapper, table, ::Pair)

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @param valueJoiner: The resulting join record
 */
fun <K, L, R, LR> KStream<K, L>.leftJoin(
    keyMapper: (K, L) -> K,
    table: GlobalKTable<K, R>,
    valueJoiner: (L, R?) -> LR
): KStream<K, LR> = leftJoin(table, keyMapper, valueJoiner)

/**
 * @param keyMapper: Map from a KStream record key to a GlobalKTable record key
 * @param table: GlobalKTable
 * @param valueJoiner: The resulting join record
 */
fun <K, L, R, KLR> KStream<K, L>.leftJoin(
    keyMapper: (K, L) -> K,
    table: GlobalKTable<K, R>,
    valueJoiner: (K, L, R?) -> KLR
): KStream<K, KLR> = leftJoin(table, keyMapper, valueJoiner)

fun <K, V, KR> KStream<K, V>.selectKey(
    name: String,
    mapper: KeyValueMapper<in K, in V, out KR>,
): KStream<KR, V> = selectKey(mapper, named(name))

/**
 * @param logValue Logs record values to secure-logs when true
 */
fun <V> KStream<String, V>.produce(topic: Topic<V>, name: String, logValue: Boolean = false) = this
    .logProduced(topic, named("log-$name"), logValue)
    .to(topic.name, topic.produced(name))

fun <V : Bufferable<V>> KStream<String, V>.produce(
    topic: Topic<V>,
    buffer: RaceConditionBuffer<String, V>,
    name: String,
    logValue: Boolean = false
) {
    val stream = logProduced(topic, named("log-$name"), logValue)
    stream.to(topic.name, topic.produced(name))
    stream.foreach(buffer::lagre)
}

/**
 * Produser records inkludert tombstones til en ktable
 * @param logValue Logs record values to secure-logs when true
 */
fun <V> KStream<String, V?>.produce(table: Table<V>, logValue: Boolean = false): KTable<String, V & Any> = this
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
): KStream<K, V?> = transformValues(
    ValueTransformerWithKeySupplier { LogProduceTable<K, V>("Produserer til KTable", table, logValue) },
    named
)

@Suppress("UNCHECKED_CAST")
fun <K, V> KTable<K, V>.filterNotNull(name: String): KTable<K, V & Any> =
    filter(name) { _, value -> value != null } as KTable<K, V & Any>

@Suppress("UNCHECKED_CAST")
fun <K, V> KStream<K, V>.filterNotNull(name: String): KStream<K, V & Any> =
    filter(name) { _, value -> value != null } as KStream<K, V & Any>

fun <K, V> KStream<K, V>.filterNotNullBy(name: String, selector: (V & Any) -> Any?): KStream<K, V & Any> =
    filterNotNullBy(name) { _, value -> selector(value) }

@Suppress("UNCHECKED_CAST")
fun <K, V> KStream<K, V>.filterNotNullBy(name: String, selector: (K, V & Any) -> Any?): KStream<K, V & Any> =
    filter(name) { key, value -> value != null && selector(key, value) != null } as KStream<K, V & Any>

fun <K, V> KTable<K, V>.filter(name: String, predicate: (K, V) -> Boolean): KTable<K, V> =
    filter(predicate, named(name))

fun <K, V> KTable<K, V>.filterKeys(name: String, predicate: (K) -> Boolean): KTable<K, V> =
    filter(name) { key, _ -> predicate(key) }

fun <K, V> KTable<K, V>.filterValues(name: String, predicate: (V) -> Boolean): KTable<K, V> =
    filter(name) { _, value -> predicate(value) }

fun <K, V> KStream<K, V>.filter(name: String, predicate: (K, V) -> Boolean): KStream<K, V> =
    filter(predicate, named(name))

fun <K, V> KStream<K, V>.filterKeys(name: String, predicate: (K) -> Boolean): KStream<K, V> =
    filter(name) { key, _ -> predicate(key) }

fun <K, V> KStream<K, V>.filterValues(name: String, predicate: (V) -> Boolean): KStream<K, V> =
    filter(name) { _, value -> predicate(value) }

fun <K, V, VR> KStream<K, V>.mapValues(name: String, mapper: (V) -> VR): KStream<K, VR> =
    mapValues(mapper, named(name))

fun <K, V, VR> KStream<K, V>.mapValues(name: String, mapper: (K, V) -> VR): KStream<K, VR> =
    mapValues(mapper, named(name))

fun <K, V, VR> KStream<K, V>.flatMapValues(name: String, mapper: (V) -> Iterable<VR>): KStream<K, VR> =
    flatMapValues(mapper, named(name))

fun <K, V, VR> KStream<K, V>.flatMapValues(name: String, mapper: (K, V) -> Iterable<VR>): KStream<K, VR> =
    flatMapValues(mapper, named(name))

fun <K, V : Iterable<VR>, VR> KStream<K, V>.flatten(): KStream<K, VR> =
    flatMapValues { value -> value }

fun <K, V : Iterable<VR>, VR> KStream<K, V>.flatten(name: String): KStream<K, VR> =
    flatMapValues(name) { value -> value }

fun <K, V, VR> KStream<K, V>.mapNotNull(name: String, mapper: (V & Any) -> VR): KStream<K, VR & Any> = this
    .filterNotNull("$name-filter-premap")
    .mapValues("$name-map", mapper)
    .filterNotNull("$name-filter-postmap")

fun <K, V, VR> KStream<K, V>.mapNotNull(name: String, mapper: (K, V & Any) -> VR): KStream<K, VR & Any> = this
    .filterNotNull("$name-filter-premap")
    .mapValues("$name-map", mapper)
    .filterNotNull("$name-filter-postmap")

fun <K, V> KStream<K, V>.peek(consumer: (V) -> Unit): KStream<K, V> =
    peek { _, value -> consumer(value) }

fun <K, V> KStream<K, V>.peek(name: String, consumer: (V) -> Unit): KStream<K, V> =
    peek(name) { _, value -> consumer(value) }

fun <K, V> KStream<K, V>.peek(name: String, consumer: (K, V) -> Unit): KStream<K, V> =
    peek(consumer, named(name))

fun <K, VL, VR> KStream<K, Pair<VL, VR>>.firstPairValue(): KStream<K, VL> =
    mapValues(Pair<VL, VR>::first)

fun <K, VL, VR> KStream<K, Pair<VL, VR>>.firstPairValue(name: String): KStream<K, VL> =
    mapValues(name, Pair<VL, VR>::first)

fun <K, VL, VR> KStream<K, Pair<VL, VR>>.secondPairValue(): KStream<K, VR> =
    mapValues(Pair<VL, VR>::second)

fun <K, VL, VR> KStream<K, Pair<VL, VR>>.secondPairValue(name: String): KStream<K, VR> =
    mapValues(name, Pair<VL, VR>::second)

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
