package org.openprojectx.kafka.mm2.autoconfigure

import org.openprojectx.kafka.mm2.core.OffsetSync
import org.openprojectx.kafka.mm2.core.OffsetTranslation
import org.springframework.scheduling.annotation.Scheduled

class OffsetMapService(
    private val repository: KafkaOffsetSyncRepository,
) {
    @Scheduled(fixedDelayString = "\${mm2.offset-map.refresh-interval:30s}")
    fun refresh(): OffsetSyncSnapshot = repository.refresh()

    fun translate(topic: String, partition: Int, offset: Long): OffsetTranslation? =
        repository.current().index.translate(topic, partition, offset)

    fun translateLatest(topic: String, partition: Int, offset: Long): OffsetTranslation? =
        repository.latest().index.translate(topic, partition, offset)

    fun syncs(topic: String?, partition: Int?): List<OffsetSync> =
        repository.current().index.syncs(topic, partition)

    fun status(): OffsetSyncSnapshot = repository.current()
}
