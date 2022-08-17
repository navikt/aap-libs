dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:3.2.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }

    implementation(project(":kafka"))

    implementation(kotlin("test"))

    implementation("io.micrometer:micrometer-registry-prometheus:1.9.3")

    testImplementation(kotlin("test"))
}
