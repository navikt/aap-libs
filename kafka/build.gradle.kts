dependencies {
    api(project(":kafka-interfaces"))

    api("org.apache.kafka:kafka-streams:3.3.1")

    implementation("ch.qos.logback:logback-classic:1.4.4")
    implementation("net.logstash.logback:logstash-logback-encoder:7.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.4")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.4")
    implementation("io.micrometer:micrometer-registry-prometheus:1.9.5")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.4")

    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.3.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}
