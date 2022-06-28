package org.apache.kafka.clients.consumer

fun <K, V> Consumer<K, V>.clientId(): String = (this as KafkaConsumer<K, V>).clientId
