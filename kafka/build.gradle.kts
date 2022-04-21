import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm")
    `maven-publish`
}

dependencies {
    api("ch.qos.logback:logback-classic:1.2.11")

    api("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.2")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.2")

    api("org.apache.kafka:kafka-clients:3.1.0")
    api("org.apache.kafka:kafka-streams:3.1.0")

    api("io.confluent:kafka-streams-avro-serde:7.0.1") {
        exclude("org.apache.kafka", "kafka-clients")
    }
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "17"
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            groupId = "no.nav.aap"
            artifactId = "kafka"
            version = "0.0.1"

            from(components["java"])
        }
    }
}

kotlin.sourceSets["main"].kotlin.srcDirs("main")
kotlin.sourceSets["test"].kotlin.srcDirs("test")
sourceSets["main"].resources.srcDirs("main")
sourceSets["test"].resources.srcDirs("test")
