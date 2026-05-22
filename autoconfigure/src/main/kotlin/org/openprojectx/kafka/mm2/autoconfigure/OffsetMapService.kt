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
        translateBatchWithIndex(request, repository.current().index)

    fun translateBatchLatest(request: BatchOffsetTranslationRequest): BatchOffsetTranslationResponse =
        translateBatchWithIndex(request, repository.latest().index)

    fun syncs(topic: String?, partition: Int?): List<OffsetSync> =
        repository.current().index.syncs(topic, partition)

    fun status(): OffsetSyncSnapshot = repository.current()

    private fun translateBatchWithIndex(
        request: BatchOffsetTranslationRequest,
        index: OffsetSyncIndex,
    ): BatchOffsetTranslationResponse {
        val startedAt = System.nanoTime()
        val partitionResults = request.offsetList
            .groupBy { it.partition }
            .map { (partition, offsets) ->
                val translations = offsets.map { offset ->
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
                            errorMessages = listOf(
                                "No offset sync found for ${request.topicName}-$partition at source offset ${offset.startOffset}",
                            ),
                            success = false,
                        )
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
