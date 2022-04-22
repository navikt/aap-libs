dependencies {
    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.2")
    implementation("io.micrometer:micrometer-registry-prometheus:1.8.5")

    api("org.apache.kafka:kafka-clients:3.1.0")
    api("org.apache.kafka:kafka-streams:3.1.0")
    api("io.confluent:kafka-streams-avro-serde:7.0.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }

    testImplementation(kotlin("test"))
}
