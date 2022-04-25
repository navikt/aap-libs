dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:3.1.0") {
        // included in :kafka
        exclude("org.rocksdb", "rocksdbjni")
    }
    implementation("io.micrometer:micrometer-registry-prometheus:1.8.5")
    implementation(project(":kafka"))
    implementation(kotlin("test"))
}
