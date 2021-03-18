package kr.jadekim.kafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.Serializer
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

suspend fun <K, V> KafkaProducer<K, V>.sendAwait(record: ProducerRecord<K, V>) = suspendCoroutine<RecordMetadata> {
    send(record) callback@{ metadata: RecordMetadata?, exception: Exception? ->
        when {
            exception != null -> it.resumeWithException(exception)
            metadata != null -> it.resume(metadata)
            else -> throw IllegalStateException()
        }
    }
}

fun <K, V> KafkaProducer<K, V>.send(
    topic: String,
    value: V,
    key: K? = null,
    partition: Int? = null,
    timestamp: Long? = null,
    headers: Iterable<Header>? = null
) = send(ProducerRecord(topic, partition, timestamp, key, value, headers))

suspend fun <K, V> KafkaProducer<K, V>.sendAwait(
    topic: String,
    value: V,
    key: K? = null,
    partition: Int? = null,
    timestamp: Long? = null,
    headers: Iterable<Header>? = null
) = sendAwait(ProducerRecord<K, V>(topic, partition, timestamp, key, value, headers))

fun <K, V> createProducer(
    config: KafkaProducerConfig,
    keySerializer: Serializer<K>? = null,
    valueSerializer: Serializer<V>? = null
) = KafkaProducer(config, keySerializer, valueSerializer)

fun <K, V> KafkaProducer<K, V>.asChannel(topic: String) = ProducingChannel(this, topic)