package kr.jadekim.kafka

import java.time.Duration
import java.util.Properties

open class KafkaConfig : MutableMap<String, Any?> by mutableMapOf() {

    fun toProperties(): Properties {
        validate()

        return Properties().apply { putAll(this@KafkaConfig.toMap()) }
    }

    open fun validate() {
        //do nothing
    }

    open fun copy() = KafkaConfig().apply { putAll(this@KafkaConfig) }

    protected val Duration.isIntRange: Boolean
        get() = toMillis() in Int.MIN_VALUE..Int.MAX_VALUE
}