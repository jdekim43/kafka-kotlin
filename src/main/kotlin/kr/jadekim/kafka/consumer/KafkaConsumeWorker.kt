package kr.jadekim.kafka.consumer

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.time.Duration

abstract class KafkaConsumeWorker<K, V>(
    val client: KafkaConsumer<K, V>,
    dispatcher: CoroutineDispatcher = Dispatchers.Default
) : CoroutineScope {

    override val coroutineContext = CoroutineName("KafkaConsumeWorker") + dispatcher

    var pollTimeout = Duration.ofMillis(500)

    private val supervisor = SupervisorJob()
    private var job: Job? = null

    open suspend fun onConsumed(record: ConsumerRecord<K, V>) {
        //do nothing
    }

    open suspend fun onConsumed(topicPartition: TopicPartition, records: List<ConsumerRecord<K, V>>) {
        for (record in records) {
            try {
                onConsumed(record)
            } catch (e: Exception) {
                client.seekAwait(topicPartition, record.offset())

                throw e
            }
        }
    }

    fun startup() {
        if (job?.isActive == true) {
            return
        }

        job = launch {
            while (isActive) {
                val records = client.pollAwait(pollTimeout)

                if (records.isEmpty) {
                    continue
                }

                withContext(NonCancellable) {
                    records.partitions().map {
                        launch(supervisor) {
                            withContext(NonCancellable) {
                                onConsumed(it, records.records(it))
                                client.commitAwait(it, records.last().offset() + 1)
                            }
                        }
                    }.joinAll()
                }
            }
        }
    }

    suspend fun shutdown() {
        job?.cancelAndJoin()
    }
}