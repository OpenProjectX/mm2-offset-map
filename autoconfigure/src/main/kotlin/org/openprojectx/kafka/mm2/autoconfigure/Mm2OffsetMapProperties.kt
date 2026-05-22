package org.openprojectx.kafka.mm2.autoconfigure

import java.time.Duration
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("mm2.offset-map")
data class Mm2OffsetMapProperties(
    val enabled: Boolean = true,
    val sourceCluster: String = "source",
    val targetCluster: String = "target",
    val bootstrapServers: String = "localhost:9093",
    val offsetSyncsTopic: String = "mm2-offset-syncs.source.internal",
    val refreshInterval: Duration = Duration.ofSeconds(30),
    val consumerProperties: Map<String, String> = emptyMap(),
)
