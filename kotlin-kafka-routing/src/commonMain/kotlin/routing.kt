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
package org.cufy.kafka.routing

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.cufy.kafka.routing.annotation.KafkaDsl

var KafkaRoute.bootstrapServers: List<String>?
    get() = properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)?.split(",")
    set(value) = properties.set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, value?.joinToString(","))

var KafkaRoute.groupId: String?
    get() = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
    set(value) = properties.set(ConsumerConfig.GROUP_ID_CONFIG, value)

@KafkaDsl
@Stable
fun KafkaRoute.route(topic: String, block: KafkaRoute.() -> Unit): KafkaRoute {
    @OptIn(ExperimentalKafkaRoutingAPI::class)
    return createRouteFromTopic(topic).apply(block)
}

@KafkaDsl
@Stable
fun KafkaRoute.consume(topic: String, block: RoutingInterceptor<KafkaEvent>): KafkaRoute {
    @OptIn(ExperimentalKafkaRoutingAPI::class)
    return createRouteFromTopic(topic).apply { handle(block) }
}

@KafkaDsl
@Stable
fun KafkaRoute.consume(block: RoutingInterceptor<KafkaEvent>): KafkaRoute {
    return apply { handle(block) }
}

@ExperimentalKafkaRoutingAPI
fun KafkaRoute.fallbackHandler(handler: RoutingInterceptor<KafkaEvent>) {
    fallbackHandlers += handler
}

@ExperimentalKafkaRoutingAPI
fun KafkaRoute.errorHandler(handler: RoutingInterceptor<Throwable>) {
    errorHandlers += handler
}

@ExperimentalKafkaRoutingAPI
fun KafkaRoute.onSetupStage(handler: RoutingInterceptor<KafkaEvent>) {
    onSetupBlocks += handler
}

@ExperimentalKafkaRoutingAPI
fun KafkaRoute.onCleanupStage(handler: RoutingInterceptor<KafkaEvent>) {
    onCleanupBlocks += handler
}
