plugins {
    kotlin("plugin.serialization")
}

val ktorVersion = "2.1.1"
dependencies {
    api("io.ktor:ktor-client-core:$ktorVersion")
    api("com.nimbusds:nimbus-jose-jwt:9.25.6")
    api("io.ktor:ktor-client-cio:$ktorVersion")
    api("io.ktor:ktor-client-content-negotiation:$ktorVersion")
    api("io.ktor:ktor-serialization-jackson:$ktorVersion")
    api("io.ktor:ktor-client-auth:$ktorVersion")

    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.4")

    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")

    testImplementation(kotlin("test"))
}