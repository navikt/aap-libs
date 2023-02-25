dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:7.3.2-ce") {
        exclude("org.apache.kafka", "kafka-clients")
    }

    implementation(project(":kafka-2"))
    implementation("org.apache.kafka:kafka-streams:3.3.1")

    implementation(kotlin("test"))

    implementation("io.micrometer:micrometer-registry-prometheus:1.10.2")
}
