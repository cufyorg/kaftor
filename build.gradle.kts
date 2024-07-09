plugins {
    kotlin("multiplatform") version libs.versions.kotlin apply false
    kotlin("plugin.serialization") version libs.versions.kotlin apply false
}

group = "org.cufy"
version = "2.0.0"

tasks.wrapper {
    gradleVersion = "8.7"
}

subprojects {
    group = "org.cufy.kafka.routing"

    repositories {
        mavenCentral()
        maven { url = uri("https://jitpack.io") }
    }
}
