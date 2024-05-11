package com.itv.video

import java.util.Scanner
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.random.Random

val godLock = Any()

fun main() {
    val brokerUrl = "tcp://localhost:1883"
    val clientId = "box-simulator2"
//    Thread {
        listenEvents(brokerUrl, clientId+"listen")
//    }.start()
//    Thread {
//        sendEvents("custom", brokerUrl, clientId+"send")
//    }.start()
}

private fun sendEvents(topic: String, brokerUrl: String, clientId: String) {
    println("Sending...")
    val scanner = Scanner(System.`in`)
    val client = MqttSender(brokerUrl, clientId)
    client.connect()
    while (true) {
        val message = scanner.nextLine()
        if (message == "quit")
            break
        client.publishWithValidation(topic, message, 1)
    }
    client.disconnect()
}

private fun listenEvents(brokerUrl: String, clientId: String) {
    val client = MqttListener(brokerUrl, clientId)
    client.eventHandlers = mapOf(
        "camera_credentials_check_events" to
                {
                    println("Creds Check gotten message: $it")
                    // Change it to send error code
                    MqttResponse(200, "Invalid creds")
                },
        "added_camera_infos" to
                {
                    println("Added camera: $it")
                    // Change it to send error code
                    if (Random.nextBoolean()) MqttResponse(200, "OK")
                    else MqttResponse(400, "Smth wrong")
                },
        "camera_settings_updates" to
                {
                    println("Updated camera: $it")
                    // Change it to send error code
                    if (Random.nextBoolean()) MqttResponse(200, "OK")
                    else MqttResponse(400, "Smth wrong")
                },

        "camera_video_events" to
                {
                    println("Video event $it")
                    // Change it to send error code
                    null
                }
    )
    client.connect()

    client.subscribeOnDefined()
//    client.subscribe(setOf("custom"))
    println("Listening")
    Thread.sleep(Long.MAX_VALUE)

    client.disconnect()
}