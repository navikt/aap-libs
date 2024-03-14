package libs.kafka.serde

import org.apache.kafka.common.serialization.Serde

interface StreamSerde<T> : Serde<T>
