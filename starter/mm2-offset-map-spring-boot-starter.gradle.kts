plugins {
    kotlin("jvm")
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.springBoot.get()}"))

    api(project(":mm2-offset-map-spring-boot-autoconfigure"))
}