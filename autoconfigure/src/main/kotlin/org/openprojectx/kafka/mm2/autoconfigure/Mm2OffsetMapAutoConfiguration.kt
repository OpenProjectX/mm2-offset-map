package org.openprojectx.kafka.mm2.autoconfigure

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling

@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(Mm2OffsetMapProperties::class)
@ConditionalOnProperty(prefix = "mm2.offset-map", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class Mm2OffsetMapAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun kafkaOffsetSyncRepository(properties: Mm2OffsetMapProperties): KafkaOffsetSyncRepository =
        KafkaOffsetSyncRepository(properties)

    @Bean
    @ConditionalOnMissingBean
    fun offsetMapService(
        repository: KafkaOffsetSyncRepository,
        properties: Mm2OffsetMapProperties,
    ): OffsetMapService =
        OffsetMapService(repository, properties)

    @Bean
    @ConditionalOnMissingBean
    fun offsetMapController(service: OffsetMapService): OffsetMapController =
        OffsetMapController(service)
}
