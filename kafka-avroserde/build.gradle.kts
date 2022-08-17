repositories {
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation(project(":kafka"))

    api("org.apache.kafka:kafka-streams:3.2.1")
    api("io.confluent:kafka-streams-avro-serde:7.2.1")

    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("net.logstash.logback:logstash-logback-encoder:7.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.3")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.3")
    implementation("io.micrometer:micrometer-registry-prometheus:1.9.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.4")

    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:7.2.1-ce") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}
