package app

import io.github.davidepianca98.MQTTClient
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.Subscription
import io.github.davidepianca98.mqtt.packets.mqttv5.SubscriptionOptions
import io.github.davidepianca98.mqtt.packets.Qos
import kotlinx.coroutines.channels.Channel

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import uniffi.dkls.NetworkInterface


class MQTTInterface(val client: MQTTClient, val topic: String): NetworkInterface {
    private val messageChannel = Channel<ByteArray>(capacity=64)

    init {
        val subscribeStatus = client.subscribe(
            subscriptions = listOf(
                Subscription(topicFilter = topic,
                    options = SubscriptionOptions(qos = Qos.AT_LEAST_ONCE, noLocal=false)
                )
            )
        )
    }

    @OptIn(ExperimentalUnsignedTypes::class)  // for using .toUByteArray
    fun handleMessage(publish: MQTTPublish) {
        if (publish.topicName == topic) {
            publish.payload?.let { uByteArray ->
                messageChannel.trySend(uByteArray.toByteArray())
            }
        }
    }

    @OptIn(ExperimentalUnsignedTypes::class)  // for using .toUByteArray
    override suspend fun send(data: ByteArray) = withContext(Dispatchers.IO){
        try {
            val payload = data.toUByteArray()

            client.publish(
                retain = false,
                topic = topic,
                qos = Qos.AT_LEAST_ONCE,
                payload = payload
            )
        } catch (e: Exception) {
            System.err.println("Failed to publish to MQTT: ${e.message}")
        }
    }

    override suspend fun receive(): ByteArray {
        return messageChannel.receive()
    }
}
