package no.nav.aap.kafka.streams.v2.serde

import org.apache.kafka.common.serialization.Serde

interface StreamSerde<T> : Serde<T>
