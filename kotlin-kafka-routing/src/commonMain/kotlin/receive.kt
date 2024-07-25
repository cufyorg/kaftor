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

import io.ktor.util.*
import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde

fun KafkaRecord.header(name: String): String? {
    return headers
        .firstOrNull { it.key().contentEquals(name, ignoreCase = true) }
        ?.value()
        ?.decodeToString()
}

@Stable
@Suppress("RedundantSuspendModifier")
suspend fun KafkaEvent.receiveText(): String {
    return record.value.decodeString()
}

@Stable
suspend inline fun <reified T> KafkaEvent.receiveJson(): T {
    return Json.decodeFromString(receiveText())
}

@Stable
suspend fun <T> KafkaEvent.receive(serde: Serde<T>): T {
    return receive(serde.deserializer())
}

@Stable
@Suppress("RedundantSuspendModifier")
suspend fun <T> KafkaEvent.receive(deserializer: Deserializer<T>): T {
    return deserializer.deserialize(record.topic, record.headers, record.value)
}
