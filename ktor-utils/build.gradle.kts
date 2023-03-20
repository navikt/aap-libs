dependencies {
    implementation("io.ktor:ktor-server-core:2.2.3")
    implementation("com.sksamuel.hoplite:hoplite-yaml:2.7.1")

    testImplementation(kotlin("test"))
    testImplementation("io.ktor:ktor-server-test-host:2.2.4")
}
