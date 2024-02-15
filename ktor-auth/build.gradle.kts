val ktorVersion = "2.3.8"

dependencies {
    implementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-auth:$ktorVersion")

    implementation("io.ktor:ktor-serialization-jackson:$ktorVersion")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.1")
    implementation("com.nimbusds:nimbus-jose-jwt:9.37.3")

    runtimeOnly("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.8.0")
}
