package org.openprojectx.kafka.mm2.autoconfigure

import org.openprojectx.kafka.mm2.core.OffsetSync
import org.openprojectx.kafka.mm2.core.OffsetTranslation
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/offsets")
class OffsetMapController(
    private val service: OffsetMapService,
) {
    @GetMapping("/translate")
    fun translate(
        @RequestParam("topic") topic: String,
        @RequestParam("partition") partition: Int,
        @RequestParam("offset") offset: Long,
    ): OffsetTranslationResponse =
        service.translate(topic, partition, offset)?.let { OffsetTranslationResponse.found(it) }
            ?: throw OffsetTranslationNotFoundException(topic, partition, offset)

    @PostMapping("/translate")
    fun translate(@RequestBody request: OffsetTranslationRequest): OffsetTranslationResponse =
        translate(request.topic, request.partition, request.offset)

    @GetMapping("/translate/latest")
    fun translateLatest(
        @RequestParam("topic") topic: String,
        @RequestParam("partition") partition: Int,
        @RequestParam("offset") offset: Long,
    ): OffsetTranslationResponse =
        service.translateLatest(topic, partition, offset)?.let { OffsetTranslationResponse.found(it) }
            ?: throw OffsetTranslationNotFoundException(topic, partition, offset)

    @PostMapping("/translate/latest")
    fun translateLatest(@RequestBody request: OffsetTranslationRequest): OffsetTranslationResponse =
        translateLatest(request.topic, request.partition, request.offset)

    @GetMapping("/syncs")
    fun syncs(
        @RequestParam(name = "topic", required = false) topic: String?,
        @RequestParam(name = "partition", required = false) partition: Int?,
    ): OffsetSyncsResponse = OffsetSyncsResponse(service.syncs(topic, partition))

    @PostMapping("/refresh")
    fun refresh(): OffsetMapStatusResponse = OffsetMapStatusResponse.from(service.refresh())

    @GetMapping("/status")
    fun status(): OffsetMapStatusResponse = OffsetMapStatusResponse.from(service.status())
}

data class OffsetTranslationRequest(
    val topic: String,
    val partition: Int,
    val offset: Long,
)

data class OffsetTranslationResponse(
    val topic: String,
    val partition: Int,
    val sourceOffset: Long,
    val targetOffset: Long,
    val syncUpstreamOffset: Long,
    val syncTargetOffset: Long,
) {
    companion object {
        fun found(translation: OffsetTranslation): OffsetTranslationResponse =
            OffsetTranslationResponse(
                topic = translation.topic,
                partition = translation.partition,
                sourceOffset = translation.sourceOffset,
                targetOffset = translation.targetOffset,
                syncUpstreamOffset = translation.syncUpstreamOffset,
                syncTargetOffset = translation.syncTargetOffset,
            )
    }
}

data class OffsetSyncsResponse(
    val syncs: List<OffsetSync>,
)

data class OffsetMapStatusResponse(
    val topic: String?,
    val refreshedAt: String?,
    val syncCount: Int,
) {
    companion object {
        fun from(snapshot: OffsetSyncSnapshot): OffsetMapStatusResponse =
            OffsetMapStatusResponse(
                topic = snapshot.topic,
                refreshedAt = snapshot.refreshedAt?.toString(),
                syncCount = snapshot.syncCount,
            )
    }
}

@ResponseStatus(HttpStatus.NOT_FOUND)
class OffsetTranslationNotFoundException(topic: String, partition: Int, offset: Long) :
    RuntimeException("No offset sync found for $topic-$partition at source offset $offset")
