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

By default, MM2 may store offset-sync records on the source cluster. This project's local Compose config sets `source->target.offset-syncs.topic.location=target`, so the app can read `mm2-offset-syncs.source.internal` from the target cluster. It also sets `source->target.offset.lag.max=1` so local tests emit offset-sync records quickly.

The service reads those records, decodes the MM2 key/value format, builds an in-memory index per topic partition, and translates a requested source offset using the nearest lower or equal offset-sync record:

```text
targetOffset = syncTargetOffset + (sourceOffset - syncUpstreamOffset)
```

## MirrorMaker 2 Internals

MirrorMaker 2 is a Kafka Connect application. For each enabled replication flow, such as `source->target`, it starts a small set of connectors and tasks:

- `MirrorSourceConnector`: consumes records from source topics and produces them to the target cluster.
- `MirrorCheckpointConnector`: reads source consumer-group offsets and emits checkpoint records that describe where consumer groups can resume on the target cluster.
- `MirrorHeartbeatConnector`: emits heartbeat records so operators can tell that replication between clusters is alive.

In this project, the main replication flow is:

```text
source Kafka topic source.orders -> MM2 MirrorSourceConnector -> target Kafka topic source.orders
```

The local Compose config uses `IdentityReplicationPolicy`, so replicated topics keep the same names. Without that policy, MM2 commonly prefixes remote topics with the source cluster alias, such as `source.source.orders`.

### MM2 Topics

With the local `source->target` flow, these topics are expected.

Application data topics:

- `source.orders`, `source.payments`, `source.audit`: source topics created for local testing.
- `source.orders`, `source.payments`, `source.audit` on the target cluster: replicated copies produced by MM2.

Offset-sync topic:

- `mm2-offset-syncs.source.internal`: binary MM2 records that map source topic-partition offsets to target offsets. This is the topic this service reads.
- Each record key contains the source topic and partition.
- Each record value contains `upstreamOffset` and target `offset`.
- MM2 does not necessarily write one record for every source record. It writes sync points when lag crosses its configured threshold. Local dev sets `source->target.offset.lag.max=1` to make sync records appear quickly.
- MM2 may store offset syncs on the source cluster by default. Local dev sets `source->target.offset-syncs.topic.location=target` so the target cluster has `mm2-offset-syncs.source.internal`, matching `mm2.offset-map.offset-syncs-topic`.

Checkpoint topic:

- `source.checkpoints.internal`: consumer-group checkpoint records for the source cluster alias.
- Checkpoints are group-oriented. They help Kafka consumers that use committed consumer-group offsets move to the target cluster.
- This project does not use checkpoint records for translation because Spark-style jobs may store offsets outside Kafka consumer groups.

Heartbeat topics:

- `heartbeats`: heartbeat records written by MM2.
- `source.heartbeats`: replicated heartbeat topic visible on the target cluster.
- Heartbeats are operational signals. They are useful for monitoring replication health, but they do not map application offsets.

Kafka Connect worker topics:

- `mm2-configs.source.internal`: Kafka Connect connector configuration state for the source-to-target worker group.
- `mm2-offsets.source.internal`: Kafka Connect task offsets. These are Connect's own progress offsets, not application offset mappings.
- `mm2-status.source.internal`: Kafka Connect connector and task status records.

Kafka cluster topic:

- `__consumer_offsets`: Kafka's internal consumer-group offset topic. MM2 can read this indirectly when producing checkpoints, but this service does not read it.

### Offset Syncs Versus Checkpoints

Offset syncs and checkpoints are easy to confuse:

- Offset syncs map raw topic-partition offsets: `topic + partition + source offset -> target offset`.
- Checkpoints map committed consumer-group offsets: `group + topic + partition + source offset -> target offset`.

This service uses offset syncs because callers provide an explicit source topic, partition, and offset. No Kafka consumer group is required.

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
