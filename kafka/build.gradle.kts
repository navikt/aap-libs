dependencies {
    api("org.apache.kafka:kafka-streams:3.2.0")
    api("io.confluent:kafka-streams-avro-serde:7.1.1")

    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.3")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.3")
    implementation("io.micrometer:micrometer-registry-prometheus:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.2")

    testImplementation(kotlin("test"))
}
