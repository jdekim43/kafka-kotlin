package kr.jadekim.kafka

import org.apache.kafka.clients.ClientDnsLookup
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.metrics.MetricsReporter
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.security.auth.SecurityProtocol
import java.time.Duration
import kotlin.reflect.KClass

open class KafkaClientConfig : KafkaConfig() {

    var bootstrapServers: List<String> = emptyList()
        @Suppress("UNCHECKED_CAST")
        get() = (get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) as? List<String>) ?: field
        set(value) { put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, value); field = value }

    var clientDnsLookup: ClientDnsLookup = ClientDnsLookup.USE_ALL_DNS_IPS
        get() = (get(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG) as? String)
            ?.let { ClientDnsLookup.forConfig(it) }
            ?: field
        set(value) { put(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, value.toString()); field = value }

    var metadataMaxAge: Duration = Duration.ofMinutes(5)
        get() = (get(CommonClientConfigs.METADATA_MAX_AGE_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, value.toMillis()); field = value }

    var sendBufferBytes: Int = 131072 //128KiB
        get() = get(CommonClientConfigs.SEND_BUFFER_CONFIG) as? Int ?: field
        set(value) { put(CommonClientConfigs.SEND_BUFFER_CONFIG, value); field = value }

    var receiveBufferBytes: Int = 32768 //32KiB
        get() = get(CommonClientConfigs.RECEIVE_BUFFER_CONFIG) as? Int ?: field
        set(value) { put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, value); field = value }

    var clientId: String = ""
        get() = get(CommonClientConfigs.CLIENT_ID_CONFIG) as? String ?: field
        set(value) { put(CommonClientConfigs.CLIENT_ID_CONFIG, value); field = value }

    var clientRack: String = ""
        get() = get(CommonClientConfigs.CLIENT_RACK_CONFIG) as? String ?: field
        set(value) { put(CommonClientConfigs.CLIENT_RACK_CONFIG, value); field = value }

    var reconnectBackoff: Duration = Duration.ofMillis(50)
        get() = (get(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, value.toMillis()); field = value }

    var reconnectBackoffMax: Duration = Duration.ofSeconds(1)
        get() = (get(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, value.toMillis()); field = value }

    var retries: Int = Int.MAX_VALUE
        get() = get(CommonClientConfigs.RETRIES_CONFIG) as? Int ?: field
        set(value) { put(CommonClientConfigs.RETRIES_CONFIG, value); field = value }

    var retryBackoff: Duration = Duration.ofMillis(100)
        get() = (get(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, value.toMillis()); field = value }

    var metricsSampleWindow: Duration = Duration.ofSeconds(30)
        get() = (get(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG, value.toMillis()); field = value }

    var metricsNumSamples: Int = 2
        get() = get(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG) as? Int ?: field
        set(value) { put(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG, value); field = value }

    var metricsRecordingLevel: Sensor.RecordingLevel = Sensor.RecordingLevel.INFO
        get() = (get(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG) as? String)
            ?.let { Sensor.RecordingLevel.forName(it) }
            ?: field
        set(value) { put(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG, value.name); field = value }

    var metricReporterClasses: List<Class<out MetricsReporter>> = emptyList()
        @Suppress("UNCHECKED_CAST")
        get() = get(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG) as? List<Class<out MetricsReporter>> ?: field
        set(value) { put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, value); field = value }

    var securityProtocol: SecurityProtocol = SecurityProtocol.forName(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL)
        get() = (get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) as? String)
            ?.let { SecurityProtocol.forName(it) }
            ?: field
        set(value) { put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, value.name); field = value }

    var socketConnectionSetupTimeout: Duration = Duration.ofMillis(CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS)
        get() = (get(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, value.toMillis()); field = value }

    var socketConnectionSetupTimeoutMax: Duration = Duration.ofMillis(CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS)
        get() = (get(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, value.toMillis()); field = value }

    var connectionsMaxIdle: Duration = Duration.ofMinutes(9)
        get() = (get(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG) as? Long)
            ?.let { Duration.ofMillis(it) }
            ?: field
        set(value) { put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, value.toMillis()); field = value }

    var requestTimeout: Duration = Duration.ofSeconds(30)
        get() = (get(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, value.toMillis().toInt()); field = value }

    var groupId: String? = null
        get() = get(CommonClientConfigs.GROUP_ID_CONFIG) as? String ?: field
        set(value) { put(CommonClientConfigs.GROUP_ID_CONFIG, value); field = value }

    var groupInstanceId: String? = null
        get() = get(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG) as? String ?: field
        set(value) { put(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG, value); field = value }

    var maxPollInterval: Duration = Duration.ofMinutes(5)
        get() = (get(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG, value.toMillis().toInt()); field = value }

    var rebalanceTimeout: Duration = Duration.ofMinutes(1)
        get() = (get(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG, value.toMillis().toInt()); field = value }

    var sessionTimeout: Duration = Duration.ofSeconds(10)
        get() = (get(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG, value.toMillis().toInt()); field = value }

    var heartbeatInterval: Duration = Duration.ofSeconds(3)
        get() = (get(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG, value.toMillis().toInt()); field = value }

    var defaultApiTimeout: Duration = Duration.ofMinutes(1)
        get() = (get(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG) as? Int)
            ?.let { Duration.ofMillis(it.toLong()) }
            ?: field
        set(value) { put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, value.toMillis().toInt()); field = value }

    fun bootstrapServer(vararg server: String) {
        bootstrapServers = bootstrapServers + server.toList()
    }

    override fun copy() = KafkaClientConfig().apply { putAll(this@KafkaClientConfig) }

    override fun validate() {
        assert(bootstrapServers.isNotEmpty())
        assert(bootstrapServers.all { it.isNotBlank() })
        assert(maxPollInterval.isIntRange)
        assert(rebalanceTimeout.isIntRange)
        assert(sessionTimeout.isIntRange)
        assert(heartbeatInterval.isIntRange)
        assert(defaultApiTimeout.isIntRange)
        assert(requestTimeout.isIntRange)
        assert(retries >= 0)
        assert(receiveBufferBytes >= CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND)
        assert(!requestTimeout.isNegative)
        assert(sendBufferBytes >= CommonClientConfigs.SEND_BUFFER_LOWER_BOUND)
        assert(!metadataMaxAge.isNegative)
        assert(metricsNumSamples >= 1)
        assert(!metricsSampleWindow.isNegative)
        assert(!reconnectBackoffMax.isNegative)
        assert(!reconnectBackoff.isNegative)
        assert(Int.MIN_VALUE <= heartbeatInterval.toMillis() && heartbeatInterval.toMillis() <= Int.MAX_VALUE)
        assert(maxPollInterval.toMillis() >= 1)
        assert(!defaultApiTimeout.isNegative)
    }
}