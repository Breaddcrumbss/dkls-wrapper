package app

import uniffi.dkls.*
import io.github.davidepianca98.MQTTClient
import io.github.davidepianca98.mqtt.MQTTVersion.MQTT5
import app.MQTTInterface
import io.github.davidepianca98.mqtt.MQTTVersion
import kotlinx.coroutines.runBlocking


object Native {
    fun loadOrThrow() {
        // Looks for "libdkls.so" (Linux) via java.library.path
        // e.g. run with: -Djava.library.path=kotlin/src/main/resources/native
        System.loadLibrary("dkls")
    }
}

fun main() {
    println("DKLS Kotlin CLI starting...")

    try {
        Native.loadOrThrow()
        println("Native library loaded: dkls")
    } catch (e: UnsatisfiedLinkError) {
        System.err.println("Failed to load native library 'dkls'.")
        System.err.println("Tip: run with -Djava.library.path=src/main/resources/native")
        throw e
    }

    // Minimal "binding is accessible" check:
    // Replace the line below with any trivial call/constant/type that exists in your generated dkls.kt.
    println("UniFFI bindings are on the classpath: ${Keyshare::class.qualifiedName}")

    println("Smoke test OK.")

    testing()
}

fun testing() = runBlocking {
    // Testing network interface
    var networkInterface: MQTTInterface? = null

    @OptIn(ExperimentalUnsignedTypes::class)  // for using .toUByteArray
    val client = MQTTClient(
        mqttVersion = MQTTVersion.MQTT5,
        address = "test.mosquitto.org",
        port = 1883,
        tls = null,
        publishReceived = {
                publish -> networkInterface?.handleMessage(publish)
        }
    )

    networkInterface = MQTTInterface(client, "test/topic/aisfghai")
    client.runSuspend()

    val tester = NetworkInterfaceTester(networkInterface)

    try {
        println("Starting relay test...")
        val testData = "Test message".encodeToByteArray()

        tester.testRelay(testData)

        println("Test Passed!")
    } catch (e: Exception) {
        println("Test Failed: ${e.message}")
    } finally {
        tester.close()
    }
}
