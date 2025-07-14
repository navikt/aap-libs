repositories {
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation(project(":kafka-streams"))

    api("org.apache.kafka:kafka-streams:3.7.0")
    api("io.confluent:kafka-streams-avro-serde:7.4.0")

    implementation("ch.qos.logback:logback-classic:1.5.18")
    implementation("net.logstash.logback:logstash-logback-encoder:8.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.19.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.19.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.19.1")
    implementation("io.micrometer:micrometer-registry-prometheus:1.14.5")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.10.2")

    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.7.0") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}

configurations.all {
    resolutionStrategy {
        force("org.apache.kafka:kafka-clients:3.7.0")
    }
}
