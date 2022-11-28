repositories {
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation(project(":kafka"))

    api("org.apache.kafka:kafka-streams:3.3.1")
    api("io.confluent:kafka-streams-avro-serde:7.3.0")

    implementation("ch.qos.logback:logback-classic:1.4.5")
    implementation("net.logstash.logback:logstash-logback-encoder:7.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.14.1")
    implementation("io.micrometer:micrometer-registry-prometheus:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.4")

    testImplementation(kotlin("test"))
    testImplementation("org.apache.kafka:kafka-streams-test-utils:3.3.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}

configurations.all {
    resolutionStrategy {
        force("org.apache.kafka:kafka-clients:3.3.1")
    }
}
