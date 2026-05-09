plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("buildsrc.convention.spring-kotlin")
}


dependencies {

    implementation(project(":mm2-offset-map-spring-boot-starter"))
//    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")

}
