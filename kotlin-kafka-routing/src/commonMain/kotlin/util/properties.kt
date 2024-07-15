package org.cufy.kafka.routing.util

import org.cufy.kafka.routing.KafkaRoute
import java.util.*
import kotlin.reflect.KProperty
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.toKotlinDuration

internal class StringAsListProperty(
    private val name: String,
    private val delimiter: String,
    private val default: () -> List<String>
) {
    private fun Properties.get(): List<String> {
        return when (val value = this[name]) {
            null -> default()
            else -> value.toString().split(delimiter)
        }
    }

    private fun Properties.set(value: List<String>) {
        this[name] = value.joinToString(delimiter)
    }

    operator fun getValue(route: KafkaRoute, property: KProperty<*>): List<String> {
        return route.properties.get()
    }

    operator fun setValue(route: KafkaRoute, property: KProperty<*>, value: List<String>) {
        route.properties.set(value)
    }
}

internal class StringProperty(
    private val name: String,
    private val default: () -> String
) {
    private fun Properties.get(): String {
        return when (val value = this[name]) {
            null -> default()
            else -> value.toString()
        }
    }

    private fun Properties.set(value: String) {
        this[name] = value
    }

    operator fun getValue(route: KafkaRoute, property: KProperty<*>): String {
        return route.properties.get()
    }

    operator fun setValue(route: KafkaRoute, property: KProperty<*>, value: String) {
        route.properties.set(value)
    }
}

internal class BooleanProperty(
    private val name: String,
    private val default: () -> Boolean
) {
    private fun Properties.get(): Boolean {
        return when (val value = this[name]) {
            true, "true" -> true
            false, "false" -> false
            else -> default()
        }
    }

    private fun Properties.set(value: Boolean) {
        this[name] = value.toString()
    }

    operator fun getValue(route: KafkaRoute, property: KProperty<*>): Boolean {
        return route.properties.get()
    }

    operator fun setValue(route: KafkaRoute, property: KProperty<*>, value: Boolean) {
        route.properties.set(value)
    }
}

internal class DurationMillisecondsProperty(
    private val name: String,
    private val default: () -> Duration
) {
    private fun Properties.get(): Duration {
        return when (val value = this[name]) {
            is String -> value.toLongOrNull()?.milliseconds ?: default()
            is Number -> value.toLong().milliseconds
            is java.time.Duration -> value.toKotlinDuration()
            is Duration -> value
            else -> default()
        }
    }

    private fun Properties.set(value: Duration) {
        this[name] = value.inWholeMilliseconds.toString()
    }

    operator fun getValue(route: KafkaRoute, property: KProperty<*>): Duration {
        return route.properties.get()
    }

    operator fun setValue(route: KafkaRoute, property: KProperty<*>, value: Duration) {
        route.properties.set(value)
    }
}
