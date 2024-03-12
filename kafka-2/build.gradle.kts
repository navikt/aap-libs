dependencies {
    implementation(project(":cache"))
    api(project(":kafka-interfaces"))

    implementation("org.apache.kafka:kafka-streams:3.5.1")

    implementation("ch.qos.logback:logback-classic:1.5.3")
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.16.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")
    implementation("io.micrometer:micrometer-registry-prometheus:1.12.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.8.0")

    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.5.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}
