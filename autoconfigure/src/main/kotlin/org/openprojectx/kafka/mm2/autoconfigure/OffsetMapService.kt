package org.openprojectx.kafka.mm2.autoconfigure

import org.openprojectx.kafka.mm2.core.OffsetSync
import org.openprojectx.kafka.mm2.core.OffsetSyncIndex
import org.openprojectx.kafka.mm2.core.OffsetTranslation
import org.springframework.scheduling.annotation.Scheduled

class OffsetMapService(
    private val repository: KafkaOffsetSyncRepository,
    private val properties: Mm2OffsetMapProperties,
) {
    @Scheduled(fixedDelayString = "\${mm2.offset-map.refresh-interval:30s}")
    fun refresh(): OffsetSyncSnapshot = repository.refresh()

    fun translate(topic: String, partition: Int, offset: Long): OffsetTranslation? =
        repository.current().index.translate(topic, partition, offset)

    fun translateLatest(topic: String, partition: Int, offset: Long): OffsetTranslation? =
        repository.latest().index.translate(topic, partition, offset)

    fun translateBatch(request: BatchOffsetTranslationRequest): BatchOffsetTranslationResponse =
        translateBatchWithIndex(request) { repository.current().index }

    fun translateBatchLatest(request: BatchOffsetTranslationRequest): BatchOffsetTranslationResponse =
        translateBatchWithIndex(request) { repository.latest().index }

    fun syncs(topic: String?, partition: Int?): List<OffsetSync> =
        repository.current().index.syncs(topic, partition)

    fun status(): OffsetSyncSnapshot = repository.current()

    private fun translateBatchWithIndex(
        request: BatchOffsetTranslationRequest,
        indexProvider: () -> OffsetSyncIndex,
    ): BatchOffsetTranslationResponse {
        val startedAt = System.nanoTime()
        val requestedPartitions = request.offsetList.map { it.partition }.distinct()
        val sourceOffsetRanges = repository.sourceOffsetRanges(request.topicName, requestedPartitions)
        val index by lazy(indexProvider)
        val partitionResults = request.offsetList
            .groupBy { it.partition }
            .map { (partition, offsets) ->
                val translations = offsets.map { offset ->
                    val range = sourceOffsetRanges[partition]
                    if (range == null) {
                        BatchOffsetTranslationResult(
                            sourceOffset = offset.startOffset,
                            targetOffset = null,
                            translationMethod = "OFFSET_SYNC",
                            errorMessages = "Source topic partition ${request.topicName}-$partition does not exist",
                            success = false,
                        )
                    } else if (!range.contains(offset.startOffset)) {
                        BatchOffsetTranslationResult(
                            sourceOffset = offset.startOffset,
                            targetOffset = null,
                            translationMethod = "OFFSET_SYNC",
                            errorMessages = "Source offset ${offset.startOffset} is out of range for " +
                                "${request.topicName}-$partition; valid range is " +
                                "[${range.beginningOffset}, ${range.endOffset})",
                            success = false,
                        )
                    } else {
                        index.translate(request.topicName, partition, offset.startOffset)
                            ?.let {
                                BatchOffsetTranslationResult(
                                    sourceOffset = offset.startOffset,
                                    targetOffset = it.targetOffset,
                                    translationMethod = "OFFSET_SYNC",
                                    errorMessages = null,
                                    success = true,
                                )
                            }
                            ?: BatchOffsetTranslationResult(
                                sourceOffset = offset.startOffset,
                                targetOffset = null,
                                translationMethod = "OFFSET_SYNC",
                                errorMessages =
                                    "No offset sync found for ${request.topicName}-$partition " +
                                        "at source offset ${offset.startOffset}",
                                success = false,
                            )
                    }
                }

                BatchPartitionTranslationResult(
                    partition = partition,
                    status = partitionStatus(translations),
                    offsetTranslations = translations,
                )
            }

        val successCount = partitionResults.sumOf { partition ->
            partition.offsetTranslations.count { it.success }
        }
        val totalRequested = request.offsetList.size

        return BatchOffsetTranslationResponse(
            topicName = request.topicName,
            sourceCluster = properties.sourceCluster,
            targetCluster = properties.targetCluster,
            partitionResults = partitionResults,
            summary = BatchOffsetTranslationSummary(
                totalRequested = totalRequested,
                successCount = successCount,
                failureCount = totalRequested - successCount,
                processTimeMs = (System.nanoTime() - startedAt) / 1_000_000,
                partitionProcessed = partitionResults.size,
            ),
        )
    }

    private fun partitionStatus(translations: List<BatchOffsetTranslationResult>): String {
        val successCount = translations.count { it.success }
        return when (successCount) {
            translations.size -> "SUCCESS"
            0 -> "FAILED"
            else -> "PARTIAL_SUCCESS"
        }
    }
}
