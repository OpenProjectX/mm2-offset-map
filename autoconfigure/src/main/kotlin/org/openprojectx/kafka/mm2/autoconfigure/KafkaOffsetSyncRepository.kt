package org.openprojectx.kafka.mm2.autoconfigure

import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.openprojectx.kafka.mm2.core.OffsetSync
import org.openprojectx.kafka.mm2.core.OffsetSyncDecoder
import org.openprojectx.kafka.mm2.core.OffsetSyncIndex

class KafkaOffsetSyncRepository(
    private val properties: Mm2OffsetMapProperties,
) {
    private val snapshot = AtomicReference(OffsetSyncSnapshot())

    fun current(): OffsetSyncSnapshot = snapshot.get()

    fun refresh(): OffsetSyncSnapshot {
        val refreshed = latest()
        snapshot.set(refreshed)
        return refreshed
    }

    fun latest(): OffsetSyncSnapshot {
        val syncs = readOffsetSyncs()
        return OffsetSyncSnapshot(
            index = OffsetSyncIndex.from(syncs),
            refreshedAt = Instant.now(),
            topic = properties.offsetSyncsTopic,
        )
    }

    fun sourceOffsetRanges(topic: String, partitions: Collection<Int>): Map<Int, SourceOffsetRange> {
        if (partitions.isEmpty()) {
            return emptyMap()
        }

        KafkaConsumer<ByteArray, ByteArray>(consumerProperties(properties.effectiveSourceBootstrapServers)).use { consumer ->
            val knownPartitions = consumer
                .partitionsFor(topic)
                .orEmpty()
                .map { it.partition() }
                .toSet()
            val topicPartitions = partitions
                .distinct()
                .filter { it in knownPartitions }
                .map { TopicPartition(topic, it) }

            if (topicPartitions.isEmpty()) {
                return emptyMap()
            }

            consumer.assign(topicPartitions)
            val beginningOffsets = consumer.beginningOffsets(topicPartitions)
            val endOffsets = consumer.endOffsets(topicPartitions)

            return topicPartitions.associate { topicPartition ->
                topicPartition.partition() to SourceOffsetRange(
                    beginningOffset = beginningOffsets[topicPartition] ?: 0L,
                    endOffset = endOffsets[topicPartition] ?: 0L,
                )
            }
        }
    }

    private fun readOffsetSyncs(): List<OffsetSync> {
        KafkaConsumer<ByteArray, ByteArray>(consumerProperties()).use { consumer ->
            val partitions = consumer
                .partitionsFor(properties.offsetSyncsTopic)
                .map { TopicPartition(it.topic(), it.partition()) }

            if (partitions.isEmpty()) {
                return emptyList()
            }

            consumer.assign(partitions)
            consumer.seekToBeginning(partitions)
            val endOffsets = consumer.endOffsets(partitions)
            val syncs = mutableListOf<OffsetSync>()

            while (partitions.any { consumer.position(it) < (endOffsets[it] ?: 0L) }) {
                consumer.poll(Duration.ofMillis(500)).forEach { record ->
                    if (record.key() != null && record.value() != null) {
                        syncs += OffsetSyncDecoder.decode(record.key(), record.value())
                    }
                }
            }

            return syncs
        }
    }

    private fun consumerProperties(bootstrapServers: String = properties.bootstrapServers): Properties =
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
            put(ConsumerConfig.GROUP_ID_CONFIG, "mm2-offset-map-${System.currentTimeMillis()}")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            putAll(properties.consumerProperties)
        }
}

data class SourceOffsetRange(
    val beginningOffset: Long,
    val endOffset: Long,
) {
    fun contains(offset: Long): Boolean = offset >= beginningOffset && offset < endOffset
}

data class OffsetSyncSnapshot(
    val index: OffsetSyncIndex = OffsetSyncIndex.empty,
    val refreshedAt: Instant? = null,
    val topic: String? = null,
) {
    val syncCount: Int = index.size()
}
