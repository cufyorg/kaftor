plugins {
    `maven-publish`

    kotlin("multiplatform")
    kotlin("plugin.serialization")
}

kotlin {
    compilerOptions {
        optIn.add("kotlin.time.ExperimentalTime")
    }
    jvm()
    sourceSets {
        commonMain {
            dependencies {
                implementation(kotlin("stdlib"))
                implementation(kotlin("reflect"))
                implementation(libs.kotlinx.serialization.json)

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
