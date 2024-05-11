package com.itv.video

import com.fasterxml.jackson.databind.ObjectMapper
import org.eclipse.paho.mqttv5.client.IMqttToken
import org.eclipse.paho.mqttv5.client.MqttActionListener
import org.eclipse.paho.mqttv5.client.MqttAsyncClient
import org.eclipse.paho.mqttv5.client.MqttCallback
import org.eclipse.paho.mqttv5.client.MqttClient
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence
import org.eclipse.paho.mqttv5.common.MqttException
import org.eclipse.paho.mqttv5.common.MqttMessage
import org.eclipse.paho.mqttv5.common.packet.MqttProperties
import java.time.Instant
import kotlin.random.Random

class MqttSender(
    private val brokerUrl: String,
    private val clientId: String,
    private val username: String = "artemis",
    private val passWord: String = "artemis"
) {

    private lateinit var client: MqttAsyncClient
    private val publishedMessages = mutableMapOf<Int, MqttMessage>()
    private val objectMapper = ObjectMapper()

    @Throws(MqttException::class)
    fun connect() {
        val options = MqttConnectionOptions().apply {
            serverURIs = arrayOf(brokerUrl)
            userName = username
            password = passWord.toByteArray()
            connectionTimeout = 5000
            maxReconnectDelay = 1000
            isAutomaticReconnect = true
        }

        client = MqttAsyncClient(brokerUrl, clientId, MemoryPersistence())
        client.connect(options).waitForCompletion()
        client.setManualAcks(true)
        client.setCallback(object : MqttCallback {
            override fun disconnected(disconnectResponse: MqttDisconnectResponse?) {
                println("Disconnected: $disconnectResponse")
            }

            override fun mqttErrorOccurred(exception: MqttException?) {
                println(exception)
            }

            override fun messageArrived(topic: String, message: MqttMessage) {
                println("Topic: $topic, message: ${String(message.payload)}, at ${Instant.now()}")

            }

            override fun deliveryComplete(token: IMqttToken?) {
                synchronized(godLock) {
                    println("Received at ${Instant.now()}")
                    println("Token.messageId: ${token!!.message}, token.granterQos: ${token.grantedQos}, Excpetion: ${token.exception}")
                    println("Token.message.id: ${token.message?.id}, Token.message.qos: ${token.message?.qos}")
                    println("Token.requestMessage.id: ${token.requestMessage?.messageId}")
                    println("Response: ${token.response}")
                }

            }

            override fun connectComplete(reconnect: Boolean, serverURI: String?) {
                println("Connected to $serverURI")
            }

            override fun authPacketArrived(reasonCode: Int, properties: MqttProperties?) {
                TODO("Not yet implemented")
            }

        })
    }

    fun publishWithValidation(topic: String, message: String, qosLevel: Int) {
        val mqttMessage = MqttMessage()
        mqttMessage.id = Random.nextInt(0, 100000)
        mqttMessage.qos = qosLevel
        println("Message sent: ${mqttMessage.id}")
        client.publish(topic, message.toByteArray(), qosLevel, true)
        publishedMessages[mqttMessage.id] = mqttMessage
    }

    fun disconnect() {
        client.disconnect()
    }
}