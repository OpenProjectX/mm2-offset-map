# syntax=docker/dockerfile:1

FROM eclipse-temurin:17-jdk AS builder

WORKDIR /workspace

# Copy the project source after .dockerignore filtering so ignored local build,
# IDE, Gradle cache, and Git metadata files are not sent into the image build.
COPY . .

RUN tar -czf /tmp/mm2-offset-map-source.tar.gz .

RUN ./gradlew --no-daemon :app:bootJar

FROM eclipse-temurin:17-jre

WORKDIR /app

RUN addgroup --system mm2-offset-map && adduser --system --ingroup mm2-offset-map mm2-offset-map

COPY --from=builder /workspace/app/build/libs/*.jar /app/mm2-offset-map.jar
COPY --from=builder /tmp/mm2-offset-map-source.tar.gz /app/source/mm2-offset-map-source.tar.gz

USER mm2-offset-map:mm2-offset-map

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/mm2-offset-map.jar"]
