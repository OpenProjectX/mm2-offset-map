package org.openprojectx.kafka.mm2.app

import org.openprojectx.kafka.mm2.autoconfigure.Mm2OffsetMapAutoConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Import


@SpringBootApplication
@Import(Mm2OffsetMapAutoConfiguration::class)
class Mm2OffsetMapApplication

fun main(args: Array<String>) {

    runApplication<Mm2OffsetMapApplication>(*args)
}
