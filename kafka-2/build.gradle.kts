dependencies {
    implementation(project(":cache"))
    api(project(":kafka-interfaces"))

    implementation("org.apache.kafka:kafka-streams:3.4.0")

    implementation("ch.qos.logback:logback-classic:1.4.7")
    implementation("net.logstash.logback:logstash-logback-encoder:7.3")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.0")
    implementation("io.micrometer:micrometer-registry-prometheus:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.7.1")

    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.4.0") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}
