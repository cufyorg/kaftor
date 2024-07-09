plugins {
    `maven-publish`

    kotlin("multiplatform")
    kotlin("plugin.serialization")
}

kotlin {
    jvm {
        withJava()
    }
    sourceSets {
        commonMain {
            dependencies {
                implementation(kotlin("stdlib"))
                implementation(kotlin("reflect"))
                implementation(libs.kotlin.serialization.core)
                implementation(libs.kotlin.serialization.json)
                implementation(libs.kotlin.datetime)

                implementation(libs.ktor.utils)
            }
        }
        commonTest {
            dependencies {
                implementation(kotlin("test"))
            }
        }
        jvmMain {
            dependencies {
                implementation(libs.kafka.clients)
                implementation(libs.kafka.streams)
            }
        }
        jvmTest {
            dependencies {
                implementation(libs.logback.core)
                implementation(libs.logback.classic)
                implementation(libs.self4j.api)
            }
        }
    }
}
