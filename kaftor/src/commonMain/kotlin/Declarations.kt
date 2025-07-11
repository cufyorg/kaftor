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

import io.ktor.util.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.record.TimestampType
import java.nio.ByteBuffer
import kotlin.time.Instant

object RoutingConfig {
    /**
     * If automatic commit was disabled and an event
     * was failed to be handled. How long to wait
     * before retrying again (in milliseconds).
     *
     * - default = 1000
     */
    const val UNHANDLED_RETRY_INTERVAL = "routing.unhandled.retry.interval"

    /**
     * If the broker return nothing in a poll, this is the
     * time to wait before polling again.
     *
     * - default = 1000
     */
    const val EMPTY_POLL_COOLDOWN = "routing.empty.poll.cooldown"

    /**
     * The maximum time to wait when polling.
     *
     * - default = 100
     */
    const val POLL_TIMEOUT = "routing.poll.timeout"
}

typealias KaftorConsumer = KafkaConsumer<String, ByteBuffer>
typealias KaftorConsumerRecord = ConsumerRecord<String, ByteBuffer>
typealias KaftorConsumerRecords = ConsumerRecords<String, ByteBuffer>
typealias RoutingInterceptor<T> = suspend KafkaRoutingContext.(T) -> Unit
typealias Logger = org.slf4j.Logger

// corresponds to PipelineContext<Unit, ApplicationCall>
sealed interface KafkaRoutingContext {
    @Stable
    val event: KafkaEvent
}

// corresponds to ApplicationCall
sealed interface KafkaEvent {
    @Stable
    val application: KafkaApplication

    @Stable
    val record: KafkaRecord

    @Stable
    val offset: KafkaOffset

    @ExperimentalKaftorAPI
    val attributes: Attributes
}

@Suppress("RedundantSuspendModifier")
@Stable
suspend fun KafkaEvent.commit() {
    @OptIn(ExperimentalKaftorAPI::class)
    offset.isCommitted = true
}

// corresponds to ApplicationRequest
sealed interface KafkaRecord {
    @Stable
    val event: KafkaEvent

    @ExperimentalKaftorAPI
    val raw: KaftorConsumerRecord

    val offset: Long
    val topic: String
    val partition: Int
    val timestamp: Instant
    val timestampType: TimestampType
    val serializedKeySize: Int
    val serializedValueSize: Int
    val key: String?
    val value: ByteBuffer
    val headers: Headers
    val leaderEpoch: Int?
}

// corresponds to ApplicationResponse
sealed interface KafkaOffset {
    @Stable
    val event: KafkaEvent

    @ExperimentalKaftorAPI
    var isCommitted: Boolean

    @ExperimentalKaftorAPI
    var error: Throwable?
}

// corresponds to ApplicationEnvironment
sealed interface KafkaEnvironment {
    /**
     * Application logger
     */
    @Stable
    val log: Logger

    /**
     * Application's root topic.
     */
    @Stable
    val rootTopic: String

    /**
     * Indicates if development mode is enabled.
     */
    val developmentMode: Boolean
}

// corresponds to ApplicationEngine
sealed interface KafkaEngine {
    /**
     * An environment used to run this engine.
     */
    @Stable
    val environment: KafkaEnvironment

    @Stable
    val application: KafkaApplication

    /**
     * Starts this engine.
     *
     * @param wait if true, then the `start` call blocks a current thread until it finishes its execution.
     * If you run `start` from the main thread with `wait = false` and nothing else blocking this thread,
     * then your application will be terminated without handling any requests.
     * @return returns this instance
     */
    @Stable
    fun start(wait: Boolean = false): KafkaEngine

    /**
     * Stops this engine.
     */
    @Stable
    fun stop()
}

// corresponds to Application
class KafkaApplication internal constructor(
    @Stable
    val environment: KafkaEnvironment,
    val developmentMode: Boolean,
) {
    @ExperimentalKaftorAPI
    val attributes: Attributes = Attributes(concurrent = true)
}

@Stable
val KafkaApplication.log get() = environment.log
