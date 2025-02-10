dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:3.7.0") {
        exclude("org.apache.kafka", "kafka-clients")
    }

    implementation(project(":kafka-streams"))
    implementation("org.apache.kafka:kafka-streams:3.7.0")

    implementation(kotlin("test"))

    implementation("io.micrometer:micrometer-registry-prometheus:1.14.4")
}
