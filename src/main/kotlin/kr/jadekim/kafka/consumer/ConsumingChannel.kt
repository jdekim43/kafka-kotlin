package kr.jadekim.kafka.consumer

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer

class ConsumingChannel<K, V> private constructor(
    val client: KafkaConsumer<K, V>,
    val topic: String,
    dispatcher: CoroutineDispatcher,
    internal val channel: Channel<ConsumerRecord<K, V>>
) : ReceiveChannel<ConsumerRecord<K, V>> by channel {
}