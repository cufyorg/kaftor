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

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteBufferDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.toJavaDuration

@ExperimentalKafkaRoutingAPI
class SimpleKafkaEnvironment internal constructor(
    override val log: Logger,
    override val rootTopic: String,
    override val developmentMode: Boolean,
) : KafkaEnvironment

@ExperimentalKafkaRoutingAPI
class SimpleKafkaEngine internal constructor(
    override val environment: KafkaEnvironment,
    private val modules: List<KafkaRoute.() -> Unit>,
    configure: KafkaEngine.Configuration.() -> Unit,
) : KafkaEngine {
    private val configuration = KafkaEngine.Configuration().apply(configure)

    private val _job = AtomicReference<Job>()
    override val application = KafkaApplication(environment, environment.developmentMode)

    override fun start(wait: Boolean): KafkaEngine {
        val job = SupervisorJob()
        val oldJob = _job.getAndSet(job)

        oldJob?.cancel()

        val root = KafkaRoute(null, EmptyRouteSelector)
        modules.forEach { root.apply(it) }

        val routes = generateSequence(root.children) { it.flatMap { it.children } }
            .takeWhile { it.isNotEmpty() }
            .flatten()
            .filter { it.handlers.isNotEmpty() }

        val scope = CoroutineScope(job)

        for (route in routes) {
            val topic = route.calculateTopic()
            val properties = route.calculateProperties()
            val handler = createCombinedHandler(route)
            val autoCommitEnabled = isAutoCommitEnabledIn(properties)

            val client = KafkaConsumer(
                /* properties = */ properties,
                /* keyDeserializer = */ StringDeserializer(),
                /* valueDeserializer = */ ByteBufferDeserializer(),
            )

            val prefixedTopic = when {
                environment.rootTopic.isEmpty() ->
                    topic

                environment.rootTopic.endsWith(".") ->
                    "${environment.rootTopic}$topic"

                else ->
                    "${environment.rootTopic}.topic"
            }

            scope.launch {
                client.use {
                    client.subscribe(listOf(prefixedTopic))

                    if (autoCommitEnabled)
                        loopCommitEnabled(client, handler)
                    else
                        loopCommitDisabled(client, handler)
                }
            }
        }

        if (wait) {
            runBlocking {
                job.join()
            }
        }

        return this
    }

    override fun stop() {
        val oldJob = _job.getAndSet(null)

        oldJob?.cancel()
    }

    private suspend fun loopCommitEnabled(
        client: RoutingKafkaConsumer,
        handler: RoutingInterceptor<Unit>,
    ) {
        while (true) {
            yield()

            val records = poll(client)

            if (records.isEmpty) continue

            coroutineScope {
                for (record in records) {
                    launch {
                        val event = createKafkaEvent(application, record)
                        val context = createKafkaRoutingContext(event)

                        handler(context, Unit)

                        // regardless if the event was commited or not
                        // auto commit was enabled thus implies that
                        // at-most-once behaviour is desired.
                        @OptIn(ExperimentalKafkaRoutingAPI::class)
                        if (!event.offset.isCommitted) {
                            val p = event.record.partition
                            val o = event.record.offset
                            val k = event.record.key

                            environment.log.warn("Unhandled event: partition=$p, offset=$o, key=$k")
                        }
                    }
                }
            }
        }
    }

    private suspend fun loopCommitDisabled(
        client: RoutingKafkaConsumer,
        handler: RoutingInterceptor<Unit>,
    ) {
        while (true) {
            yield()

            val records = poll(client)

            if (records.isEmpty) continue

            coroutineScope {
                for (record in records) {
                    launch {
                        val event = createKafkaEvent(application, record)
                        val context = createKafkaRoutingContext(event)

                        while (true) {
                            handler(context, Unit)

                            // auto commit was disabled, at-least-once behaviour
                            // is desired. Commit the event only if the event was
                            // explicitly declared it was commited.

                            @OptIn(ExperimentalKafkaRoutingAPI::class)
                            if (event.offset.isCommitted) {
                                commitSync(client)
                                break
                            }

                            val timeout = configuration.unhandledRetryInterval

                            environment.log.info("Unhandled event: retrying in $timeout")

                            delay(timeout)
                        }
                    }
                }
            }
        }
    }

    private suspend fun poll(client: RoutingKafkaConsumer): RoutingConsumerRecords {
        return withContext(Dispatchers.IO) {
            try {
                val timeout = configuration.pollTimeout

                client.poll(timeout.toJavaDuration())
            } catch (e: Exception) {
                e.printStackTrace()
                ConsumerRecords.empty()
            } catch (e: Throwable) {
                e.printStackTrace()
                throw e
            }
        }
    }

    private suspend fun commitSync(client: RoutingKafkaConsumer) {
        withContext(Dispatchers.IO) {
            try {
                client.commitSync()
            } catch (e: Exception) {
                e.printStackTrace()
            } catch (e: Throwable) {
                e.printStackTrace()
                throw e
            }
        }
    }
}
