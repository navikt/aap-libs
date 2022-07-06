dependencies {
    api("org.apache.kafka:kafka-streams:7.2.0-ce")
    api("io.confluent:kafka-streams-avro-serde:7.1.1")

    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("net.logstash.logback:logstash-logback-encoder:7.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.3")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.3")
    implementation("io.micrometer:micrometer-registry-prometheus:1.9.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.3")

    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.2.0") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}
