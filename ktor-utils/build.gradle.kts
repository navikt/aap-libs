dependencies {
    implementation("io.ktor:ktor-server-core:2.0.0")
    implementation("com.sksamuel.hoplite:hoplite-yaml:2.1.2")

    testImplementation(kotlin("test"))
}

sourceSets["test"].resources.srcDirs("test")