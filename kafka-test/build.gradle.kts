dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:3.2.0") {
        exclude("org.apache.kafka", "kafka-clients")
    }

    implementation(project(":kafka"))

    implementation(kotlin("test"))

    implementation("io.micrometer:micrometer-registry-prometheus:1.9.1")
    implementation("io.confluent:kafka-streams-avro-serde:7.1.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }

    testImplementation(kotlin("test"))
}
