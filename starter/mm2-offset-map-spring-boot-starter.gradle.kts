plugins {
    kotlin("jvm")
    `java-library`
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

dependencies {
    api(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.springBoot.get()}"))

    api(project(":mm2-offset-map-spring-boot-autoconfigure"))
}
