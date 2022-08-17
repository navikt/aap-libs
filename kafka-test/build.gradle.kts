dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:7.2.1-ce") {
        exclude("org.apache.kafka", "kafka-clients")
    }

    implementation(project(":kafka"))

    implementation(kotlin("test"))

    implementation("io.micrometer:micrometer-registry-prometheus:1.9.3")

    testImplementation(kotlin("test"))
}
