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

import org.cufy.kaftor.utils.dsl.KaftorDsl

@KaftorDsl
@Stable
fun KafkaRoute.route(topic: String, block: KafkaRoute.() -> Unit): KafkaRoute {
    @OptIn(ExperimentalKaftorAPI::class)
    return createRouteFromTopic(topic).apply(block)
}

@KaftorDsl
@Stable
fun KafkaRoute.consume(topic: String, block: RoutingInterceptor<KafkaEvent>): KafkaRoute {
    @OptIn(ExperimentalKaftorAPI::class)
    return createRouteFromTopic(topic).apply { handle(block) }
}

@KaftorDsl
@Stable
fun KafkaRoute.consume(block: RoutingInterceptor<KafkaEvent>): KafkaRoute {
    return apply { handle(block) }
}

@ExperimentalKaftorAPI
fun KafkaRoute.fallbackHandler(handler: RoutingInterceptor<KafkaEvent>) {
    fallbackHandlers += handler
}

@ExperimentalKaftorAPI
fun KafkaRoute.errorHandler(handler: RoutingInterceptor<Throwable>) {
    errorHandlers += handler
}

@ExperimentalKaftorAPI
fun KafkaRoute.onSetupStage(handler: RoutingInterceptor<KafkaEvent>) {
    onSetupBlocks += handler
}

@ExperimentalKaftorAPI
fun KafkaRoute.onCleanupStage(handler: RoutingInterceptor<KafkaEvent>) {
    onCleanupBlocks += handler
}
