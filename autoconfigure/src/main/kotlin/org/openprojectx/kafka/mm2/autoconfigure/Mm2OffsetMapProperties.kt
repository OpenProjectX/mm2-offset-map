package org.openprojectx.kafka.mm2.autoconfigure

import java.time.Duration
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("mm2.offset-map")
class Mm2OffsetMapProperties {
    var enabled: Boolean = true
    var sourceCluster: String = "source"
    var targetCluster: String = "target"
    var bootstrapServers: String = "localhost:9093"
    var sourceBootstrapServers: String? = null
    var offsetSyncsTopic: String = "mm2-offset-syncs.source.internal"
    var refreshInterval: Duration = Duration.ofSeconds(30)
    var consumerProperties: Map<String, String> = emptyMap()
    var targetConsumerProperties: Map<String, String> = emptyMap()
    var sourceConsumerProperties: Map<String, String> = emptyMap()

    val effectiveSourceBootstrapServers: String
        get() = sourceBootstrapServers?.takeIf { it.isNotBlank() } ?: bootstrapServers

    val effectiveTargetConsumerProperties: Map<String, String>
        get() = consumerProperties + targetConsumerProperties

    val effectiveSourceConsumerProperties: Map<String, String>
        get() = consumerProperties + sourceConsumerProperties
}
