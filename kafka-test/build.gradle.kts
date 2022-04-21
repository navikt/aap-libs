dependencies {
    api(project(":kafka"))
    api(kotlin("test"))
    api("org.apache.kafka:kafka-streams-test-utils:3.1.0")
}

kotlin.sourceSets["main"].kotlin.srcDirs("main")
kotlin.sourceSets["test"].kotlin.srcDirs("test")
