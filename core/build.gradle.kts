plugins {
    id("buildsrc.convention.kotlin-jvm")
}


dependencies {

    api(libs.kafkaClients)
    testImplementation(kotlin("test"))


}
