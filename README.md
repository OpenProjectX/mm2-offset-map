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
    source-cluster: source
    target-cluster: target
    bootstrap-servers: localhost:9093
    source-bootstrap-servers: localhost:9092
    offset-syncs-topic: mm2-offset-syncs.source.internal
    refresh-interval: 30s
    consumer-properties:
      security.protocol: PLAINTEXT
    target-consumer-properties: {}
    source-consumer-properties: {}
```

`bootstrap-servers` must point to the Kafka cluster that contains the offset-sync topic. With the provided Compose stack that is the target cluster. `source-bootstrap-servers` points to the source cluster and is used by batch APIs to check the source topic partition offset range before attempting translation.

Use `consumer-properties` for Kafka client settings shared by both clusters, such as SASL, SSL, timeouts, or custom authentication. Use `target-consumer-properties` for the target cluster that stores the MM2 offset-sync topic, and `source-consumer-properties` for the source cluster used by batch APIs to validate source topic partition offset ranges. Source and target values override shared values with the same Kafka client property name.

## Docker Usage

Build the runnable application image:

```bash
docker build -t mm2-offset-map:local .
```

Run it against Kafka brokers exposed on the host. With the local defaults, the target cluster contains the MM2 offset-sync topic on `localhost:9093`, and the source cluster is on `localhost:9092`:

```bash
docker run --rm \
  --network host \
  -e MM2_OFFSET_MAP_BOOTSTRAP_SERVERS=localhost:9093 \
  -e MM2_OFFSET_MAP_SOURCE_BOOTSTRAP_SERVERS=localhost:9092 \
  -e MM2_OFFSET_MAP_OFFSET_SYNCS_TOPIC=mm2-offset-syncs.source.internal \
  -p 8080:8080 \
  mm2-offset-map:local
```

If you run the container on a Docker bridge network instead of `--network host`, use broker hostnames that are reachable from inside the container, for example `target-kafka:29092` and `source-kafka:29092`.

### Docker Kafka Authentication

Kafka client properties can be passed through environment variables. Use `MM2_OFFSET_MAP_CONSUMER_PROPERTIES_*` for shared defaults, `MM2_OFFSET_MAP_TARGET_CONSUMER_PROPERTIES_*` for the target cluster, and `MM2_OFFSET_MAP_SOURCE_CONSUMER_PROPERTIES_*` for the source cluster. For Kafka properties that contain dots, such as `sasl.jaas.config`, `SPRING_APPLICATION_JSON` is the most reliable Docker-friendly format.

Different source and target SASL/SCRAM credentials:

```bash
docker run --rm --network host \
  -e 'SPRING_APPLICATION_JSON={
    "mm2": {
      "offset-map": {
        "bootstrap-servers": "target.example.com:9093",
        "source-bootstrap-servers": "source.example.com:9093",
        "target-consumer-properties": {
          "security.protocol": "SASL_SSL",
          "sasl.mechanism": "SCRAM-SHA-512",
          "sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"target-user\" password=\"target-pass\";"
        },
        "source-consumer-properties": {
          "security.protocol": "SASL_SSL",
          "sasl.mechanism": "SCRAM-SHA-512",
          "sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"source-user\" password=\"source-pass\";"
        }
      }
    }
  }' \
  -p 8080:8080 \
  mm2-offset-map:local
```

Kerberos requires mounting `krb5.conf` and keytabs into the container, setting JVM Kerberos config, and passing Kafka SASL properties. Source and target can use different principals and keytabs:

```bash
docker run --rm --network host \
  -v /secure/krb5.conf:/etc/krb5.conf:ro \
  -v /secure/source.keytab:/app/secrets/source.keytab:ro \
  -v /secure/target.keytab:/app/secrets/target.keytab:ro \
  -e JAVA_TOOL_OPTIONS='-Djava.security.krb5.conf=/etc/krb5.conf' \
  -e 'SPRING_APPLICATION_JSON={
    "mm2": {
      "offset-map": {
        "bootstrap-servers": "target.example.com:9093",
        "source-bootstrap-servers": "source.example.com:9093",
        "target-consumer-properties": {
          "security.protocol": "SASL_SSL",
          "sasl.mechanism": "GSSAPI",
          "sasl.kerberos.service.name": "kafka",
          "sasl.jaas.config": "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/app/secrets/target.keytab\" principal=\"target-client@EXAMPLE.COM\";"
        },
        "source-consumer-properties": {
          "security.protocol": "SASL_SSL",
          "sasl.mechanism": "GSSAPI",
          "sasl.kerberos.service.name": "kafka",
          "sasl.jaas.config": "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"/app/secrets/source.keytab\" principal=\"source-client@EXAMPLE.COM\";"
        }
      }
    }
  }' \
  -p 8080:8080 \
  mm2-offset-map:local
```

If the brokers also require TLS trust material, mount the truststore and add the relevant `SSL_TRUSTSTORE_*` Kafka properties to the source, target, or shared property group.

Check the service:

```bash
curl http://localhost:8080/api/v1/offsets/status
curl -X POST http://localhost:8080/api/v1/offsets/refresh
```

The image also contains a compressed copy of the project source at:

```text
/app/source/mm2-offset-map-source.tar.gz
```

That archive is created from the Docker build context after `.dockerignore` filtering, so ignored local build outputs, IDE files, Gradle cache files, and Git metadata are excluded.

## Helm Usage

The Helm chart is in `charts/mm2-offset-map`.

Install with default values:

```bash
helm upgrade --install mm2-offset-map ./charts/mm2-offset-map \
  --namespace mm2-offset-map \
  --create-namespace
```

Set the image and Kafka endpoints:

```bash
helm upgrade --install mm2-offset-map ./charts/mm2-offset-map \
  --namespace mm2-offset-map \
  --set image.repository=ghcr.io/openprojectx/mm2-offset-map \
  --set image.tag=0.1.0-SNAPSHOT \
  --set config.mm2.offset-map.bootstrap-servers=target-kafka.kafka.svc.cluster.local:9092 \
  --set config.mm2.offset-map.source-bootstrap-servers=source-kafka.kafka.svc.cluster.local:9092
```

Enable Istio and select the gateway:

```yaml
istio:
  virtualService:
    enabled: true
    hosts:
      - mm2-offset-map.example.com
    gateways:
      - istio-system/public-gateway
```

Use `configSecret` when Kafka credentials should come from a Kubernetes Secret instead of rendered Helm values. See `charts/mm2-offset-map/README.md` for SASL/Kerberos examples and gateway options.

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
- `GET /api/v1/offsets/translate/latest`: no-cache translate. Reads the offset-sync topic from Kafka immediately, builds a temporary index, calculates the translation, and does not update the cached snapshot.
- `POST /api/v1/offsets/translate/latest`: JSON body version of no-cache translate.
- `POST /api/v1/offsets/translate/batch`: cached batch translation. Request body contains one `topicName` and an `offsetList` of `partition` plus `startOffset` values.
- `POST /api/v1/offsets/translate/batch/latest`: no-cache batch translation. Reads the offset-sync topic from Kafka immediately once, builds a temporary index, and translates all requested offsets from that index.
- `GET /api/v1/offsets/syncs`: list loaded sync records, optionally filtered by `topic` and `partition`.
- `POST /api/v1/offsets/refresh`: refresh the in-memory index from Kafka immediately.
- `GET /api/v1/offsets/status`: return current offset-sync topic, last refresh time, and loaded sync count.

Batch request:

```json
{
  "topicName": "source.orders",
  "offsetList": [
    {
      "partition": 0,
      "startOffset": 10
    },
    {
      "partition": 1,
      "startOffset": 15
    }
  ]
}
```

Batch response groups offsets by partition and includes per-offset success or failure. `errorMessages` is a single string when an item fails and `null` when it succeeds. Batch requests check the source topic partition offset range first; offsets outside `[beginningOffset, endOffset)` fail immediately without reading the offset-sync index.

```json
{
  "topicName": "source.orders",
  "sourceCluster": "source",
  "targetCluster": "target",
  "partitionResults": [
    {
      "partition": 0,
      "status": "SUCCESS",
      "offsetTranslations": [
        {
          "sourceOffset": 10,
          "targetOffset": 10,
          "translationMethod": "OFFSET_SYNC",
          "errorMessages": null,
          "success": true
        }
      ]
    }
  ],
  "summary": {
    "totalRequested": 1,
    "successCount": 1,
    "failureCount": 0,
    "processTimeMs": 12,
    "partitionProcessed": 1
  },
  "timestamp": "2026-05-22T00:00:00Z"
}
```

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
