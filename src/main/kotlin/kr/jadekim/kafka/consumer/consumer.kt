package kr.jadekim.kafka.consumer

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import java.time.Duration
import java.util.Optional
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

suspend fun <K, V> KafkaConsumer<K, V>.commitAwait() = suspendCoroutine<Map<TopicPartition, OffsetAndMetadata>> {
    commitAsync callback@{ offsets: Map<TopicPartition, OffsetAndMetadata>?, exception: Exception? ->
        when {
            exception != null -> it.resumeWithException(exception)
            offsets != null -> it.resume(offsets)
            else -> throw IllegalStateException()
        }
    }
}

suspend fun <K, V> KafkaConsumer<K, V>.commitAwait(
    offsets: Map<TopicPartition, OffsetAndMetadata>
) = suspendCoroutine<Map<TopicPartition, OffsetAndMetadata>> {
    commitAsync(offsets) callback@{ offsets: Map<TopicPartition, OffsetAndMetadata>?, exception: Exception? ->
        when {
            exception != null -> it.resumeWithException(exception)
            offsets != null -> it.resume(offsets)
            else -> throw IllegalStateException()
        }
    }
}

suspend fun <K, V> KafkaConsumer<K, V>.commitAwait(
    topicPartition: TopicPartition,
    offset: OffsetAndMetadata
) = commitAwait(mapOf(topicPartition to offset))

suspend fun <K, V> KafkaConsumer<K, V>.commitAwait(
    topicPartition: TopicPartition,
    offset: Long,
    metadata: String = "",
    leaderEpoch: Int? = null
) = commitAwait(topicPartition, OffsetAndMetadata(offset, Optional.ofNullable(leaderEpoch), metadata))

fun <K, V> createConsumer(
    config: KafkaConsumerConfig,
    keyDeserializer: Deserializer<K>? = null,
    valueDeserializer: Deserializer<V>? = null
) = KafkaConsumer(config, keyDeserializer, valueDeserializer)

suspend fun <K, V> KafkaConsumer<K, V>.pollAwait(
    timeout: Duration,
    dispatcher: CoroutineDispatcher? = Dispatchers.IO
) = if (dispatcher == null) {
    poll(timeout)
} else {
    withContext(dispatcher) {
        poll(timeout)
    }
}

suspend fun <K, V> KafkaConsumer<K, V>.seekAwait(
    topicPartition: TopicPartition,
    offset: OffsetAndMetadata,
    dispatcher: CoroutineDispatcher? = Dispatchers.IO
) = if (dispatcher == null) {
    seek(topicPartition, offset)
} else {
    withContext(dispatcher) {
        seek(topicPartition, offset)
    }
}

suspend fun <K, V> KafkaConsumer<K, V>.seekAwait(
    topicPartition: TopicPartition,
    offset: Long,
    dispatcher: CoroutineDispatcher? = Dispatchers.IO
) = if (dispatcher == null) {
    seek(topicPartition, offset)
} else {
    withContext(dispatcher) {
        seek(topicPartition, offset)
    }
}