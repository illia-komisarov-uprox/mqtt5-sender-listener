package com.itv.video

import com.fasterxml.jackson.databind.ObjectMapper
import org.eclipse.paho.mqttv5.client.IMqttToken
import org.eclipse.paho.mqttv5.client.MqttAsyncClient
import org.eclipse.paho.mqttv5.client.MqttCallback
import org.eclipse.paho.mqttv5.client.MqttClient
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence
import org.eclipse.paho.mqttv5.common.MqttException
import org.eclipse.paho.mqttv5.common.MqttMessage
import org.eclipse.paho.mqttv5.common.packet.MqttProperties

class MqttListener(
    private val brokerUrl: String,
    private val clientId: String,
) {
    lateinit var eventHandlers: Map<String, (String) -> MqttResponse?>
    private lateinit var client: MqttClient
    private val objectMapper = ObjectMapper()

    fun connect() {
        val options = MqttConnectionOptions().apply {
            serverURIs = arrayOf(brokerUrl)
            userName = "artemis"
            password = "artemis".toByteArray()
            connectionTimeout = 5000
            maxReconnectDelay = 1000
            isAutomaticReconnect = true
        }

        client = MqttClient(brokerUrl, clientId)
        client.setCallback(object : MqttCallback {
            override fun disconnected(disconnectResponse: MqttDisconnectResponse?) {
                println("Disconnected: $disconnectResponse")
            }

            override fun mqttErrorOccurred(exception: MqttException?) {
                println(exception)
            }

            override fun messageArrived(topic: String, message: MqttMessage) {

                    val event = String(message.payload)
                    val handler = eventHandlers[topic]

                synchronized(godLock) {
                    println("Message gotten from $topic: $event")
                }

                    if (handler != null) {
                        val response = handler(event)
                        response?.let { sendResponse(topic, message, response) }
                    } else {
                        Thread.sleep(6000)
                        synchronized(godLock) {
                            println("acknowledgement for message ID: ${message.id}, QoS: ${message.qos}")
                        }

                        client.messageArrivedComplete(message.id, message.qos)
                    }
            }

            override fun deliveryComplete(token: IMqttToken?) {
                println("Token: $token")
            }

            override fun connectComplete(reconnect: Boolean, serverURI: String?) {
                println("Connected to $serverURI")
            }

            override fun authPacketArrived(reasonCode: Int, properties: MqttProperties?) {
                TODO("Not yet implemented")
            }

        })

        client.connect(options)

    }

    fun disconnect() {
        client.disconnect()
    }

    fun subscribe(topics: Set<String>) {
        client.subscribe(topics.toTypedArray(), IntArray(topics.size) { 1 })
    }

    fun subscribeOnDefined() {
        client.subscribe(eventHandlers.keys.toTypedArray(), IntArray(eventHandlers.keys.size) { 1 })
    }

    fun sendResponse(initTopic: String, msgFromTopic: MqttMessage, response: MqttResponse) {
        val ackMessage = MqttMessage(objectMapper.writeValueAsBytes(response)).apply {
            val mqttProperties =
                MqttProperties().apply {
                    correlationData = msgFromTopic.properties.correlationData
                }
            properties = mqttProperties
            qos = msgFromTopic.qos
            isRetained = msgFromTopic.isRetained
        }
        try {
            client.publish("${initTopic}_response", ackMessage)
            println("Sent acknowledgement $response for topic: $initTopic")
        } catch (e: MqttException) {
            println("Error sending acknowledgement: $e")
        }
    }
}

data class MqttResponse(val code: Int, val message: String)