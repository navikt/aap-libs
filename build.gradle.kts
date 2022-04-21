plugins {
    kotlin("jvm") version "1.6.21" apply false
}

subprojects {
    repositories {
        maven("https://packages.confluent.io/maven/")
        maven("'https://jitpack.io'")
        mavenCentral()
    }
}
