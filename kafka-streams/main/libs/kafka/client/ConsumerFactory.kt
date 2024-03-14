package libs.kafka.client

import libs.kafka.StreamsConfig
import libs.kafka.Topic
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

enum class OffsetResetPolicy {
    earliest,
    latest
}

interface ConsumerFactory {
    fun <V : Any> createConsumer(
        streamsConfig: StreamsConfig,
        topic: Topic<V>,
        maxEstimatedProcessingTimeMs: Long, // e.g. 4_000
        groupIdSuffix: Int = 1, // used to "reset" the consumer by registering a new
        offsetResetPolicy: OffsetResetPolicy = OffsetResetPolicy.earliest
    ): Consumer<String, V> {
        val consumerConfig = ConsumerFactoryConfig(
            streamsConfig = streamsConfig,
            clientId = "${streamsConfig.applicationId}-consumer-${topic.name}",
            groupId = "${streamsConfig.applicationId}-${topic.name}-$groupIdSuffix",
            maxEstimatedProcessingTimeMs,
            offsetResetPolicy
        )

        return KafkaConsumer(
            consumerConfig.toProperties(),
            topic.keySerde.deserializer(),
            topic.valueSerde.deserializer()
        )
    }
}

private const val TWO_MIN_MS: Long = 120_000

private class ConsumerFactoryConfig(
    private val streamsConfig: StreamsConfig,
    private val clientId: String,
    private val groupId: String,
    private val maxEstimatedProcessingTimeMs: Long,
    private val autoOffset: OffsetResetPolicy,
) {

    fun toProperties(): Properties = Properties().apply {
        this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = streamsConfig.brokers
        this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = autoOffset.name
        this[CommonClientConfigs.CLIENT_ID_CONFIG] = clientId
        this[ConsumerConfig.GROUP_ID_CONFIG] = groupId

        /**
         * Set to 2min + estimated max processing time
         * If max estimated processing time is 4 sec, set it to 124_000
         */
        this[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = TWO_MIN_MS + maxEstimatedProcessingTimeMs

        streamsConfig.ssl?.let { ssl ->
            putAll(ssl.properties())
        }

        streamsConfig.schemaRegistry?.let { schema ->
            putAll(schema.properties())
        }
    }
}
