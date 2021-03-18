package kr.jadekim.kafka.producer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header

class ProducingChannel<K, V> private constructor(
    val client: KafkaProducer<K, V>,
    val topic: String,
    internal val maxWorkerCount: Int,
    dispatcher: CoroutineDispatcher,
    internal val channel: Channel<ProducerRecord<K, V>>
) : SendChannel<ProducerRecord<K, V>> by channel {

    constructor(
        client: KafkaProducer<K, V>,
        topic: String,
        queueSize: Int = Channel.RENDEZVOUS,
        maxWorkerCount: Int = 8,
        dispatcher: CoroutineDispatcher = Dispatchers.Main
    ) : this(client, topic, maxWorkerCount, dispatcher, Channel(queueSize))

    var produceExceptionHandler: ((ProducerRecord<K, V>, Throwable) -> Unit)? = null

    private val workerScope = CoroutineScope(CoroutineName("ProducingChannel-$topic") + dispatcher)
    private val workers = mutableListOf<Job>()

    private var currentSendingCount = 0
    private var isFullWorker = false

    suspend fun send(
        value: V,
        key: K? = null,
        partition: Int? = null,
        timestamp: Long? = null,
        headers: Iterable<Header>? = null
    ) {
        if (!isFullWorker && workers.size <= currentSendingCount) {
            launchWorker()
        }

        send(ProducerRecord(topic, partition, timestamp, key, value, headers))
    }

    private fun launchWorker() {
        if (isFullWorker) {
            return
        }

        synchronized(workers) {
            if (workers.size >= maxWorkerCount) {
                return
            }

            workers += workerScope.launch {
                for (record in channel) {
                    currentSendingCount += 1
                    try {
                        client.sendAwait(record)
                    } catch (e: Exception) {
                        produceExceptionHandler?.invoke(record, e)
                    } finally {
                        currentSendingCount -= 1
                    }
                }
            }

            isFullWorker = workers.size >= maxWorkerCount
        }
    }
}