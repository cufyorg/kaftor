/*
 *	Copyright 2024 cufy.org
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *	    http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */
package org.cufy.kaftor

import java.util.*

class KafkaRoute(val parent: KafkaRoute?, val selector: KafkaRouteSelector) {
    private val _children = mutableListOf<KafkaRoute>()
    val children: List<KafkaRoute> get() = _children

    internal val hierarchy = generateSequence(this) { it.parent }.toList()

    internal val onSetupBlocks = mutableListOf<RoutingInterceptor<KafkaEvent>>()
    internal val onCleanupBlocks = mutableListOf<RoutingInterceptor<KafkaEvent>>()

    internal val handlers = mutableListOf<RoutingInterceptor<KafkaEvent>>()
    internal val fallbackHandlers = mutableListOf<RoutingInterceptor<KafkaEvent>>()
    internal val errorHandlers = mutableListOf<RoutingInterceptor<Throwable>>()

    val properties: Properties = Properties()

    @Stable
    fun createChild(segments: KafkaRouteSelector): KafkaRoute {
        val existingEntry = _children.firstOrNull { it.selector == segments }
        if (existingEntry == null) {
            val entry = KafkaRoute(this, segments)
            _children += entry
            return entry
        }
        return existingEntry
    }

    /**
     * Allows using a route instance for building additional routes.
     */
    @Stable
    operator fun invoke(body: KafkaRoute.() -> Unit): Unit = body()

    /**
     * Installs a handler into this route which is called when the route is selected for a call.
     */
    @Stable
    fun handle(handler: RoutingInterceptor<KafkaEvent>) {
        handlers += handler
    }

    override fun toString(): String {
        return when (parent) {
            null -> "$selector"
            else -> "$parent.$selector"
        }
    }
}

@ExperimentalKaftorAPI
fun KafkaRoute.createRouteFromTopic(topic: String): KafkaRoute {
    val parts = topic.split(".")
    var current: KafkaRoute = this
    for (index in parts.indices) {
        val value = parts[index]
        val selector = TopicSegmentConstantRouteSelector(value)

        // there may already be entry with same selector, so join them
        current = current.createChild(selector)
    }
    return current
}

sealed interface KafkaRouteSelector

data class TopicSegmentConstantRouteSelector(val value: String) : KafkaRouteSelector {
    override fun toString() = value
}

@ExperimentalKaftorAPI
data object EmptyRouteSelector : KafkaRouteSelector
