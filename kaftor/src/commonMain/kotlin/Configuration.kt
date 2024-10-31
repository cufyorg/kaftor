package org.cufy.kaftor

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.cufy.kaftor.utils.BooleanProperty
import org.cufy.kaftor.utils.DurationMillisecondsProperty
import org.cufy.kaftor.utils.StringAsListProperty
import org.cufy.kaftor.utils.StringProperty
import kotlin.time.Duration.Companion.milliseconds

var KafkaRoute.bootstrapServers by StringAsListProperty(
    name = ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
    delimiter = ",",
    default = { error("bootstrap.servers is not set") }
)

var KafkaRoute.groupId by StringProperty(
    name = ConsumerConfig.GROUP_ID_CONFIG,
    default = { error("group.id is not set") }
)

var KafkaRoute.autoCommitEnabled by BooleanProperty(
    name = ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
    default = { true }
)

var KafkaRoute.pollTimeout by DurationMillisecondsProperty(
    name = RoutingConfig.POLL_TIMEOUT,
    default = { 100.milliseconds }
)

var KafkaRoute.emptyPollCooldown by DurationMillisecondsProperty(
    name = RoutingConfig.EMPTY_POLL_COOLDOWN,
    default = { 1000.milliseconds }
)

var KafkaRoute.unhandledRetryInterval by DurationMillisecondsProperty(
    name = RoutingConfig.UNHANDLED_RETRY_INTERVAL,
    default = { 1000.milliseconds }
)
