dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:3.5.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }

    implementation(project(":kafka-2"))
    implementation("org.apache.kafka:kafka-streams:3.5.1")

    implementation(kotlin("test"))

    implementation("io.micrometer:micrometer-registry-prometheus:1.12.4")
}
