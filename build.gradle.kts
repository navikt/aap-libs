import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    `maven-publish`
    `java-library`
}

allprojects {
    repositories {
        maven("https://jitpack.io")
        mavenCentral()
    }
}

subprojects {
    group = "com.github.navikt"
    version = "1.0.0-SNAPSHOT"

    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "maven-publish")
    apply(plugin = "java-library")

    tasks {
        withType<KotlinCompile> {
            kotlinOptions.jvmTarget = "18"
        }
        withType<Jar> {
           duplicatesStrategy = DuplicatesStrategy.INCLUDE
        }
        withType<Test> {
            useJUnitPlatform()
        }
    }

    java {
        withSourcesJar()
        withJavadocJar()
    }

    publishing {
        publications {
            create<MavenPublication>("mavenJava") {
                artifactId = project.name
                from(components["java"])
            }
        }
    }

    kotlin.sourceSets["main"].kotlin.srcDirs("main")
    kotlin.sourceSets["test"].kotlin.srcDirs("test")
    sourceSets["main"].resources.srcDirs("main")
    sourceSets["test"].resources.srcDirs("test")
}
