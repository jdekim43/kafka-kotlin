package kr.jadekim.kafka.consumer

import kr.jadekim.kafka.KafkaClientConfig
import kr.jadekim.kafka.producer.KafkaProducerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.serialization.Deserializer
import java.time.Duration
import java.util.Locale

open class KafkaConsumerConfig : KafkaClientConfig() {

    var maxPollRecords: Int = 500
        get() = get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG) as? Int ?: field
        set(value) { put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value); field = value }

    var enableAutoCommit: Boolean = true
        get() = get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) as? Boolean ?: field
        set(value) { put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, value); field = value }

    var autoCommitInterval: Duration = Duration.ofSeconds(5)
        get() = (get(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, value.toMillis().toInt()); field = value }

    var partitionAssignmentStrategy: List<Class<out ConsumerPartitionAssignor>> = listOf(org.apache.kafka.clients.consumer.RangeAssignor::class.java)
        @Suppress("UNCHECKED_CAST")
        get() = get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG) as? List<Class<out ConsumerPartitionAssignor>> ?: field
        set(value) { put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, value); field = value }

    var autoOffsetReset: OffsetResetStrategy = OffsetResetStrategy.LATEST
        get() = (get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) as? String)
            ?.let { OffsetResetStrategy.valueOf(it.toUpperCase(Locale.ROOT)) }
            ?: field
        set(value) { put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value.toString()); field = value }

    var fetchMinBytes: Int = 1
        get() = get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG) as? Int ?: field
        set(value) { put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, value); field = value }

    var fetchMaxBytes: Int = ConsumerConfig.DEFAULT_FETCH_MAX_BYTES
        get() = get(ConsumerConfig.FETCH_MAX_BYTES_CONFIG) as? Int ?: field
        set(value) { put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, value); field = value }

    var fetchMaxWait: Duration = Duration.ofMillis(500)
        get() = (get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, value.toMillis().toInt()); field = value }

    var maxPartitionFetchBytes: Int = ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES
        get() = get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG) as? Int ?: field
        set(value) { put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, value); field = value }

    var checkCrcs: Boolean = true
        get() = get(ConsumerConfig.CHECK_CRCS_CONFIG) as? Boolean ?: field
        set(value) { put(ConsumerConfig.CHECK_CRCS_CONFIG, value); field = value }

    var keyDeserializerClass: Class<out Deserializer<*>>? = null
        @Suppress("UNCHECKED_CAST")
        get() = get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) as? Class<out Deserializer<*>> ?: field
        set(value) { put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, value); field = value }

    var valueDeserializerClass: Class<out Deserializer<*>>? = null
        @Suppress("UNCHECKED_CAST")
        get() = get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) as? Class<out Deserializer<*>> ?: field
        set(value) { put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, value); field = value }

    var interceptorClasses: List<Class<out ConsumerInterceptor<*, *>>> = emptyList()
        @Suppress("UNCHECKED_CAST")
        get() = get(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG) as? List<Class<out ConsumerInterceptor<*, *>>> ?: field
        set(value) { put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, value); field = value }

    var excludeInternalTopics: Boolean = true
        get() = get(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG) as? Boolean ?: field
        set(value) { put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, value); field = value }

    var isolationLevel: IsolationLevel = parseIsolationLevel(ConsumerConfig.DEFAULT_ISOLATION_LEVEL)
        get() = (get(ConsumerConfig.ISOLATION_LEVEL_CONFIG) as? String)?.let { parseIsolationLevel(it) } ?: field
        set(value) { put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, value.toKafkaValue()); field = value }

    var allowAutoCreateTopics: Boolean = ConsumerConfig.DEFAULT_ALLOW_AUTO_CREATE_TOPICS
        get() = get(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG) as? Boolean ?: field
        set(value) { put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, value); field = value }

    override fun copy() = KafkaConsumerConfig().apply { putAll(this@KafkaConsumerConfig) }

    override fun validate() {
        super.validate()
        assert(autoCommitInterval.isIntRange)
        assert(fetchMaxWait.isIntRange)
        assert(fetchMinBytes >= 0)
        assert(maxPartitionFetchBytes >= 0)
        assert(fetchMaxBytes >= 0)
        assert(maxPollRecords >= 1)
        assert(!autoCommitInterval.isNegative)
        assert(!fetchMaxWait.isNegative)
    }

    private fun parseIsolationLevel(kafkaValue: String) = IsolationLevel.valueOf(kafkaValue.toUpperCase(Locale.ROOT))

    private fun IsolationLevel.toKafkaValue() = toString().toLowerCase(Locale.ROOT)
}