repositories {
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation(project(":kafka-2"))

    api("org.apache.kafka:kafka-streams:3.5.1")
    api("io.confluent:kafka-streams-avro-serde:7.4.0")

    implementation("ch.qos.logback:logback-classic:1.4.14")
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.16.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")
    implementation("io.micrometer:micrometer-registry-prometheus:1.12.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.7.3")

    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.5.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}

configurations.all {
    resolutionStrategy {
        force("org.apache.kafka:kafka-clients:3.5.1")
    }
}
