dependencies {
    api("org.apache.kafka:kafka-streams-test-utils:3.1.0") {
        exclude("org.rocksdb", "rocksdbjni") // included in :kafka
    }

    implementation("io.micrometer:micrometer-registry-prometheus:1.8.5")
    implementation(project(":kafka"))
    implementation(kotlin("test"))

    testImplementation(kotlin("test"))
}
