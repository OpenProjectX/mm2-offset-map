package org.openprojectx.kafka.mm2.core

data class SourcePartition(
    val topic: String,
    val partition: Int,
)

data class OffsetSync(
    val topic: String,
    val partition: Int,
    val upstreamOffset: Long,
    val targetOffset: Long,
) {
    val sourcePartition: SourcePartition = SourcePartition(topic, partition)
}

data class OffsetTranslation(
    val topic: String,
    val partition: Int,
    val sourceOffset: Long,
    val targetOffset: Long,
    val syncUpstreamOffset: Long,
    val syncTargetOffset: Long,
)
