dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:3.1.0")
    implementation(kotlin("test"))
    implementation(project(":kafka"))
}
