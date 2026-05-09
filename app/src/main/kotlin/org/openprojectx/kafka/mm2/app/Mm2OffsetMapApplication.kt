package org.openprojectx.kafka.mm2.app

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication


@SpringBootApplication
class Mm2OffsetMapApplication

fun main(args: Array<String>) {

    runApplication<Mm2OffsetMapApplication>(*args)
}