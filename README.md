# kotlin-kafka-routing

Ktor-like routing for kafka consumers

### Note

Setting `auto.commit.enabled=true` will cause the implementation
to follow an **at-most-once** offset commit behaviour. If an event
is not explicitly committed (with `event.commit()`) in any installed
handler, the implementation will **do nothing** and continue polling
next events as if the unhandled event was actually committed.

On the other hand, setting `auto.commit.enabled=false` will cause
the implementation to follow an **at-least-once** offset commit
behaviour. If an event is not explicitly committed (with `event.commit()`)
in any installed handler, the implementation will **continuously retry**
handling the event by re-invoking the handlers until the event gets
explicitly committed (with `event.commit()`) before continuing polling
next events.

### Install

The main way of installing this library is
using `jitpack.io`

```kts
repositories {
    // ...
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    // Replace TAG with the desired version
    implementation("org.cufy.kotlin-kafka-routing:kotlin-kafka-routing:TAG")
}
```

### Usage

Here is a simple setup:

```kotlin
const val PROPERTIES = "# Additional Properties"

fun main() {
    embeddedConsumer(module = KafkaRoute::module)
        .start(wait = true)
}

fun KafkaRoute.module() {
    bootstrapServers = listOf(
        "server1.kafka.example.com:59002",
        "server2.kafka.example.com:59012",
        "server3.kafka.example.com:59022",
    )
    groupId = "dev.example.com:main_group"
    properties["auto.commit.enabled"] = "false"
    properties.load(PROPERTIES.reader())

    // registers a handler when any handler in this
    // route or any sub-routes has thrown an error
    @OptIn(ExperimentalKafkaRoutingAPI::class)
    errorHandler {
        it.printStackTrace()
        event.commit()
    }

    // registers a handler when any handler in this
    // route or any sub-routes is left unhandled
    @OptIn(ExperimentalKafkaRoutingAPI::class)
    fallbackHandler {
        val p = event.record.partition
        val o = event.record.offset
        val k = event.record.key

        println("Unhandled event: partition=$p, offset=$o, key=$k")
        event.commit()
    }

    // register handler for topic `random-words`
    consume(topic = "random-words") {
        val decoded = event.receiveText()

        println("Random Word: $decoded")
        event.commit()
    }

    // register handler for topic `random-numbers`
    consume(topic = "random-numbers") {
        val decoded = event.receive(IntegerSerde())

        println("Random Number: $decoded")
        event.commit()
    }

    // register handler for topic `random-objects`
    consume(topic = "random-objects") {
        val decoded = event.receiveJson<JsonObject>()

        println("Random Object: $decoded")
        event.commit()
    }

    route(topic = "com.example") {
        // scoped properties for this route
        properties["auto.commit.enabled"] = "true"

        // register handler for topic `com.example.event`
        consume(topic = "event") {
            // ...
        }
    }
}
```
