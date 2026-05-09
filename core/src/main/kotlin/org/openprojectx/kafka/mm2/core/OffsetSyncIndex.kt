package org.openprojectx.kafka.mm2.core

import java.util.NavigableMap
import java.util.TreeMap

class OffsetSyncIndex private constructor(
    private val mappings: Map<SourcePartition, NavigableMap<Long, Long>>,
) {
    fun translate(topic: String, partition: Int, sourceOffset: Long): OffsetTranslation? {
        val partitionMappings = mappings[SourcePartition(topic, partition)] ?: return null
        val sync = partitionMappings.floorEntry(sourceOffset) ?: return null
        val targetOffset = sync.value + (sourceOffset - sync.key)

        return OffsetTranslation(
            topic = topic,
            partition = partition,
            sourceOffset = sourceOffset,
            targetOffset = targetOffset,
            syncUpstreamOffset = sync.key,
            syncTargetOffset = sync.value,
        )
    }

    fun syncs(topic: String? = null, partition: Int? = null): List<OffsetSync> =
        mappings
            .asSequence()
            .filter { (sourcePartition, _) -> topic == null || sourcePartition.topic == topic }
            .filter { (sourcePartition, _) -> partition == null || sourcePartition.partition == partition }
            .flatMap { (sourcePartition, offsets) ->
                offsets.asSequence().map { (upstreamOffset, targetOffset) ->
                    OffsetSync(
                        topic = sourcePartition.topic,
                        partition = sourcePartition.partition,
                        upstreamOffset = upstreamOffset,
                        targetOffset = targetOffset,
                    )
                }
            }
            .sortedWith(compareBy<OffsetSync> { it.topic }.thenBy { it.partition }.thenBy { it.upstreamOffset })
            .toList()

    fun size(): Int = mappings.values.sumOf { it.size }

    companion object {
        val empty = OffsetSyncIndex(emptyMap())

        fun from(syncs: Iterable<OffsetSync>): OffsetSyncIndex {
            val mappings = mutableMapOf<SourcePartition, NavigableMap<Long, Long>>()

            syncs.forEach { sync ->
                mappings
                    .getOrPut(sync.sourcePartition) { TreeMap() }
                    .put(sync.upstreamOffset, sync.targetOffset)
            }

            return OffsetSyncIndex(mappings)
        }
    }
}
