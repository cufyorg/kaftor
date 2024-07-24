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
@file:OptIn(ExperimentalKafkaRoutingAPI::class)

package org.cufy.kafka.routing

import io.ktor.util.*
import kotlinx.datetime.Instant
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.record.TimestampType
import java.nio.ByteBuffer
import java.util.*
import kotlin.jvm.optionals.getOrNull

/* ============= ------------------ ============= */

internal fun createKafkaRoutingContext(
    event: KafkaEvent
): KafkaRoutingContext {
    return KafkaRoutingContextImpl(
        event = event,
    )
}

private data class KafkaRoutingContextImpl(
    override val event: KafkaEvent,
) : KafkaRoutingContext

/* ============= ------------------ ============= */

internal fun createKafkaEvent(
    application: KafkaApplication,
    record: RoutingConsumerRecord,
): KafkaEvent {
    return KafkaEventImpl(
        application = application,
        record = record,
    )
}

private class KafkaEventImpl(
    override val application: KafkaApplication,
    record: RoutingConsumerRecord,
) : KafkaEvent {
    override val record = createKafkaRecord(this, record)
    override val offset = createKafkaOffset(this)
    override val attributes = Attributes(concurrent = true)
}

/* ============= ------------------ ============= */

internal fun createKafkaRecord(
    event: KafkaEvent,
    record: RoutingConsumerRecord
): KafkaRecord {
    return KafkaRecordImpl(
        event = event,
        raw = record,
        offset = record.offset(),
        topic = record.topic(),
        partition = record.partition(),
        timestamp = Instant.fromEpochMilliseconds(record.timestamp()),
        timestampType = record.timestampType(),
        serializedKeySize = record.serializedKeySize(),
        serializedValueSize = record.serializedValueSize(),
        key = record.key(),
        value = record.value(),
        headers = record.headers(),
        leaderEpoch = record.leaderEpoch().getOrNull(),
    )
}

private data class KafkaRecordImpl(
    override val event: KafkaEvent,
    override val raw: RoutingConsumerRecord,
    override val offset: Long,
    override val topic: String,
    override val partition: Int,
    override val timestamp: Instant,
    override val timestampType: TimestampType,
    override val serializedKeySize: Int,
    override val serializedValueSize: Int,
    override val key: String,
    override val value: ByteBuffer,
    override val headers: Headers,
    override val leaderEpoch: Int?,
) : KafkaRecord

/* ============= ------------------ ============= */

internal fun createKafkaOffset(
    event: KafkaEvent
): KafkaOffset {
    return KafkaOffsetImpl(event)
}

private data class KafkaOffsetImpl(
    override val event: KafkaEvent
) : KafkaOffset {
    override var isCommitted: Boolean = false
    override var error: Throwable? = null
}

/* ============= ------------------ ============= */

internal fun KafkaRoute.calculateTopic(): String {
    val segments = mutableListOf<String>()

    for (route in hierarchy.asReversed()) {
        when (val selector = route.selector) {
            is EmptyRouteSelector -> {}

            is TopicSegmentConstantRouteSelector ->
                segments += selector.value
        }
    }

    return segments.joinToString(".")
}

internal fun KafkaRoute.calculateProperties(): Properties {
    val properties = Properties()
    for (route in hierarchy.asReversed()) {
        properties.putAll(route.properties)
    }
    return properties
}
