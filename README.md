# MM2 Offset Map

REST API for translating Kafka MirrorMaker 2 source offsets to target-cluster offsets.

This is useful for applications that manage offsets outside Kafka consumer groups, such as Spark jobs. During a cluster flip, the app can call this API with its last source-cluster topic/partition/offset and resume from the corresponding target-cluster offset.

## Modules

- `core`: MM2 offset-sync decoder and in-memory offset translation index.
- `mm2-offset-map-spring-boot-autoconfigure`: Spring Boot auto-configuration, Kafka reader, scheduler, and REST controller.
- `mm2-offset-map-spring-boot-starter`: reusable starter dependency.
- `app`: runnable local Spring Boot application.

## How It Works

MirrorMaker 2 writes offset sync records to an internal topic named like:

```text
mm2-offset-syncs.<source-cluster-alias>.internal
```

The service reads those records from the target cluster, decodes the MM2 key/value format, builds an in-memory index per topic partition, and translates a requested source offset using the nearest lower or equal offset-sync record:

```text
targetOffset = syncTargetOffset + (sourceOffset - syncUpstreamOffset)
```

## Configuration

Default app configuration is in `app/src/main/resources/application.yaml`.

```yaml
mm2:
  offset-map:
    bootstrap-servers: localhost:9093
    offset-syncs-topic: mm2-offset-syncs.source.internal
    refresh-interval: 30s
    consumer-properties:
      security.protocol: PLAINTEXT
```

Use `consumer-properties` for Kafka client settings such as SASL, SSL, timeouts, or custom authentication.

## REST API

Translate a source offset:

```http
GET /api/v1/offsets/translate?topic=source.orders&partition=0&offset=123
```

Response:

```json
{
  "topic": "source.orders",
  "partition": 0,
  "sourceOffset": 123,
  "targetOffset": 456,
  "syncUpstreamOffset": 120,
  "syncTargetOffset": 453
}
```

Other endpoints:

- `POST /api/v1/offsets/translate`: JSON body version of translate.
- `GET /api/v1/offsets/syncs`: list loaded sync records, optionally filtered by `topic` and `partition`.
- `POST /api/v1/offsets/refresh`: refresh the in-memory index from Kafka immediately.
- `GET /api/v1/offsets/status`: return current offset-sync topic, last refresh time, and loaded sync count.

## Local Development

Start the plaintext Kafka/MM2/Kafka UI stack:

```bash
docker compose up -d
```

The stack starts:

- source Kafka on `localhost:9092`
- target Kafka on `localhost:9093`
- MirrorMaker 2 replicating `source.orders`, `source.payments`, and `source.audit`
- Kafbat UI at `http://localhost:7080`

Run the app:

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew :app:bootRun
```

Verify that the controller is registered:

```bash
curl http://localhost:8080/api/v1/offsets/status
curl http://localhost:8080/actuator/mappings
```

Produce source data, then refresh and query:

```bash
docker compose exec source-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server source-kafka:29092 \
  --topic source.orders
```

After MirrorMaker has emitted offset syncs:

```bash
curl -X POST http://localhost:8080/api/v1/offsets/refresh
curl 'http://localhost:8080/api/v1/offsets/translate?topic=source.orders&partition=0&offset=0'
```

## JetBrains HTTP Client

Use `requests.http` from IntelliJ IDEA or other JetBrains IDEs. It includes requests for status, refresh, sync listing, and both GET/POST translation calls.

The default environment is stored in `http-client.env.json`.

## Test

```bash
env GRADLE_USER_HOME=/data/.gradle ./gradlew test
docker compose config
```
