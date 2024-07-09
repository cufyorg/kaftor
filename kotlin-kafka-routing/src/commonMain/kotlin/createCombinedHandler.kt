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

@OptIn(ExperimentalKafkaRoutingAPI::class)
internal fun createCombinedHandler(route: KafkaRoute): RoutingInterceptor<Unit> {
    val onSetupBlocks = route.hierarchy.flatMap { it.onSetupBlocks }.toList()
    val onCleanupBlocks = route.hierarchy.flatMap { it.onCleanupBlocks }.toList()
    val fallbackHandlers = route.hierarchy.flatMap { it.fallbackHandlers }.toList()
    val errorHandlers = route.hierarchy.flatMap { it.errorHandlers }.toList()
    val handlers = route.handlers.toList()

    return it@{
        suspend fun doCatch(error: Throwable) {
            event.offset.error = error

            for (errorHandler in errorHandlers) {
                if (event.offset.isCommitted) return
                errorHandler(this, error)
            }

            if (event.offset.isCommitted) return
            error.printStackTrace()
        }

        suspend fun doCleanup() {
            for (onCleanupBlock in onCleanupBlocks) {
                try {
                    onCleanupBlock(this, event)
                } catch (error: Throwable) {
                    doCatch(error)
                }
            }
        }

        for (onSetupBlock in onSetupBlocks) {
            try {
                onSetupBlock(this, event)
            } catch (error: Exception) {
                doCatch(error)
                return@it
            } finally {
                doCleanup()
            }
        }

        for (handler in handlers + fallbackHandlers) {
            try {
                if (event.offset.isCommitted) return@it
                handler(this, event)
            } catch (error: Exception) {
                doCatch(error)
                return@it
            } finally {
                doCleanup()
            }
        }
    }
}
