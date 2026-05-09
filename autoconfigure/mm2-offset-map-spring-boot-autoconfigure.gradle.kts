plugins {
    id("buildsrc.convention.kotlin-jvm")
    `kotlin-kapt`
}


dependencies {

    api(project(":core"))

    val bootBom = platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.springBoot.get()}")

    implementation(bootBom)
    kapt(bootBom)

    implementation("org.springframework.boot:spring-boot-autoconfigure")
    api("org.springframework.boot:spring-boot-starter")
    kapt("org.springframework.boot:spring-boot-configuration-processor")



}