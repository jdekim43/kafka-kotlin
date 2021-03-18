package kr.jadekim.kafka.producer

import kr.jadekim.kafka.KafkaClientConfig
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.serialization.Serializer
import java.time.Duration

open class KafkaProducerConfig : KafkaClientConfig() {

    enum class Acks(val configValue: String) {
        NONE("0"),
        LEADER("1"),
        IN_SYNC_REPLICAS("all"); //equally to -1

        companion object {

            fun configValueOf(value: String) = values().first { it.configValue == value }
        }
    }

    var metadataMaxIdle: Duration = Duration.ofMinutes(5)
        get() = (get(ProducerConfig.METADATA_MAX_IDLE_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, value.toMillis()); field = value }

    var batchSize: Int = 16384
        get() = get(ProducerConfig.BATCH_SIZE_CONFIG) as? Int ?: field
        set(value) { put(ProducerConfig.BATCH_SIZE_CONFIG, value); field = value }

    var acks: Acks = Acks.LEADER
        get() = (get(ProducerConfig.ACKS_CONFIG) as? String)?.let { Acks.configValueOf(it) } ?: field
        set(value) { put(ProducerConfig.ACKS_CONFIG, value.configValue); field = value }

    var linger: Duration = Duration.ZERO
        get() = (get(ProducerConfig.LINGER_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(ProducerConfig.LINGER_MS_CONFIG, value.toMillis()); field = value }

    var deliveryTimeout: Duration = Duration.ofMinutes(2)
        get() = (get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, value.toMillis().toInt()); field = value }

    var maxRequestSize: Int = 1048576
        get() = get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG) as? Int ?: field
        set(value) { put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, value); field = value }

    var maxBlock: Duration = Duration.ofMinutes(1)
        get() = (get(ProducerConfig.MAX_BLOCK_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(ProducerConfig.MAX_BLOCK_MS_CONFIG, value.toMillis()); field = value }

    var bufferMemory: Long = 33554432
        get() = get(ProducerConfig.BUFFER_MEMORY_CONFIG) as? Long ?: field
        set(value) { put(ProducerConfig.BUFFER_MEMORY_CONFIG, value); field = value }

    var compressionType: CompressionType = CompressionType.NONE
        get() = (get(ProducerConfig.COMPRESSION_TYPE_CONFIG) as? String)
            ?.let { CompressionType.forName(it) }
            ?: field
        set(value) { put(ProducerConfig.COMPRESSION_TYPE_CONFIG, value.name); field = value }

    var maxInFlightRequestsPerConnection: Int = 5
        get() = get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION) as? Int ?: field
        set(value) { put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, value); field = value }

    var keySerializerClass: Class<out Serializer<*>>? = null
        @Suppress("UNCHECKED_CAST")
        get() = get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) as? Class<out Serializer<*>> ?: field
        set(value) { put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value); field = value }

    var valueSerializerClass: Class<out Serializer<*>>? = null
        @Suppress("UNCHECKED_CAST")
        get() = get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) as? Class<out Serializer<*>> ?: field
        set(value) { put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value); field = value }

    var partitionerClass: Class<out Partitioner> = org.apache.kafka.clients.producer.internals.DefaultPartitioner::class.java
        @Suppress("UNCHECKED_CAST")
        get() = get(ProducerConfig.PARTITIONER_CLASS_CONFIG) as? Class<out Partitioner> ?: field
        set(value) { put(ProducerConfig.PARTITIONER_CLASS_CONFIG, value); field = value }

    var interceptorClasses: List<Class<out ProducerInterceptor<*, *>>> = emptyList()
        @Suppress("UNCHECKED_CAST")
        get() = get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG) as? List<Class<out ProducerInterceptor<*, *>>> ?: field
        set(value) { put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, value); field = value }

    var enableIdempotence: Boolean = false
        get() = get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG) as? Boolean ?: field
        set(value) { put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, value); field = value }

    var transactionTimeout: Duration = Duration.ofMinutes(1)
        get() = (get(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, value.toMillis().toInt()); field = value }

    var transactionalId: String? = null
        get() = get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) as? String ?: field
        set(value) { put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, value); field = value }

    override fun copy() = KafkaProducerConfig().apply { putAll(this@KafkaProducerConfig) }

    override fun validate() {
        super.validate()
        assert(deliveryTimeout.isIntRange)
        assert(transactionTimeout.isIntRange)
        assert(bufferMemory >= 0)
        assert(batchSize >= 0)
        assert(!deliveryTimeout.isNegative && deliveryTimeout.toMillis() <= Int.MAX_VALUE)
        assert(!linger.isNegative)
        assert(!maxBlock.isNegative)
        assert(maxRequestSize >= 0)
        assert(maxInFlightRequestsPerConnection >= 1)
        assert(metadataMaxIdle >= Duration.ofMillis(5000))
        assert(Int.MIN_VALUE <= transactionTimeout.toMillis() && transactionTimeout.toMillis() <= Int.MAX_VALUE)
    }
}