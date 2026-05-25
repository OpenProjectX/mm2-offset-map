# MM2 Offset Map Helm Chart

Deploys the MM2 Offset Map Spring Boot application with an optional Istio `VirtualService`.

## Install

```bash
helm upgrade --install mm2-offset-map ./charts/mm2-offset-map \
  --namespace mm2-offset-map \
  --create-namespace
```

The chart runs the container as numeric UID/GID `10001` by default. This avoids Kubernetes `runAsNonRoot` validation failures with images that declare a named non-root user.

## Kafka Configuration

Set `config.mm2.offset-map.bootstrap-servers` to the Kafka cluster that contains the MM2 offset-sync topic. Set `config.mm2.offset-map.source-bootstrap-servers` to the source Kafka cluster used by batch APIs to validate source topic partition offset ranges.

```yaml
config:
  mm2:
    offset-map:
      bootstrap-servers: target-kafka.kafka.svc.cluster.local:9092
      source-bootstrap-servers: source-kafka.kafka.svc.cluster.local:9092
      offset-syncs-topic: mm2-offset-syncs.source.internal
      consumer-properties:
        security.protocol: PLAINTEXT
      target-consumer-properties: {}
      source-consumer-properties: {}
```

`consumer-properties` are shared defaults. `target-consumer-properties` and `source-consumer-properties` can override those defaults when the two Kafka clusters use different authentication.

## Kafka Credential Options

The chart supports four Kubernetes-friendly ways to configure Kafka credentials.

1. Plain Helm values:

```yaml
config:
  mm2:
    offset-map:
      target-consumer-properties:
        security.protocol: SASL_SSL
        sasl.mechanism: SCRAM-SHA-512
      source-consumer-properties:
        security.protocol: SASL_SSL
        sasl.mechanism: SCRAM-SHA-512
```

Use this for non-sensitive settings or values injected by a secure deployment pipeline.

2. Mounted Secret containing `application.yaml`:

```bash
kubectl -n kafka create secret generic mm2-offset-map-application-yaml \
  --from-file=application.yaml=./application.yaml
```

Example `application.yaml`:

```yaml
spring:
  application:
    name: mm2-offset-map

management:
  endpoints:
    web:
      exposure:
        include: health,info,mappings

mm2:
  offset-map:
    source-cluster: source
    target-cluster: target
    bootstrap-servers: kafka-kafka-standby-0-external:9092
    source-bootstrap-servers: kafka-kafka-primary-0-external:9092
    offset-syncs-topic: mm2-offset-syncs.source.internal
    refresh-interval: 30s
    target-consumer-properties:
      security.protocol: SASL_PLAINTEXT
      sasl.mechanism: PLAIN
      sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="target-secret";
    source-consumer-properties:
      security.protocol: SASL_PLAINTEXT
      sasl.mechanism: PLAIN
      sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="source-secret";
```

Enable the mount in Helm values:

```yaml
applicationYamlSecret:
  name: mm2-offset-map-application-yaml
  key: application.yaml
  mountPath: /app/config/application.yaml
```

This is the most readable option for complex Kafka auth because it keeps the same YAML shape as local development and avoids JSON escaping.

3. `configSecret` with `SPRING_APPLICATION_JSON`:

```bash
kubectl -n kafka create secret generic mm2-offset-map-config \
  --from-literal=SPRING_APPLICATION_JSON='{
  "mm2": {
    "offset-map": {
      "bootstrap-servers": "kafka-kafka-standby-0-external:9092",
      "source-bootstrap-servers": "kafka-kafka-primary-0-external:9092",
      "target-consumer-properties": {
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"target-secret\";"
      },
      "source-consumer-properties": {
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"source-secret\";"
      }
    }
  }
}'
```

```yaml
configSecret:
  name: mm2-offset-map-config
  key: SPRING_APPLICATION_JSON
```

This is the recommended option for passwords and JAAS strings because Kafka property names keep their dots exactly.

When using map properties, do not include the property name inside the value. This is correct:

```json
"sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";"
```

This is wrong and will fail with `Login module control flag is not available in the JAAS config`:

```json
"sasl.jaas.config": "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";"
```

4. Mounted Kubernetes Secrets for files plus `applicationYamlSecret`, `configSecret`, or values for Kafka properties:

```yaml
env:
  - name: JAVA_TOOL_OPTIONS
    value: -Djava.security.krb5.conf=/etc/krb5.conf

extraVolumes:
  - name: kerberos-config
    secret:
      secretName: kerberos-config
  - name: kafka-keytabs
    secret:
      secretName: kafka-keytabs

extraVolumeMounts:
  - name: kerberos-config
    mountPath: /etc/krb5.conf
    subPath: krb5.conf
    readOnly: true
  - name: kafka-keytabs
    mountPath: /app/secrets
    readOnly: true
```

Use mounted files for Kerberos keytabs, `krb5.conf`, TLS truststores, and keystores. Reference those mounted paths from `sasl.jaas.config`, `ssl.truststore.location`, or `ssl.keystore.location`.

Credential precedence is:

```text
target Kafka consumer = consumer-properties + target-consumer-properties
source Kafka consumer = consumer-properties + source-consumer-properties
```

Shared settings go in `consumer-properties`; different source/target credentials go in the specific maps.

## Istio VirtualService

Enable the `VirtualService` and select the gateway or gateways for the environment:

```yaml
istio:
  virtualService:
    enabled: true
    hosts:
      - mm2-offset-map.example.com
    gateways:
      - istio-system/public-gateway
    match:
      - uri:
          prefix: /
```

Multiple gateways are supported:

```yaml
istio:
  virtualService:
    enabled: true
    gateways:
      - istio-system/public-gateway
      - mesh
```

## Kerberos

Mount `krb5.conf` and keytabs, then pass JVM and Kafka SASL settings through `configSecret` or values:

```yaml
env:
  - name: JAVA_TOOL_OPTIONS
    value: -Djava.security.krb5.conf=/etc/krb5.conf

extraVolumes:
  - name: kerberos-config
    secret:
      secretName: kerberos-config
  - name: kafka-keytabs
    secret:
      secretName: kafka-keytabs

extraVolumeMounts:
  - name: kerberos-config
    mountPath: /etc/krb5.conf
    subPath: krb5.conf
    readOnly: true
  - name: kafka-keytabs
    mountPath: /app/secrets
    readOnly: true
```
