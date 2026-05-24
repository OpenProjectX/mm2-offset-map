# MM2 Offset Map Helm Chart

Deploys the MM2 Offset Map Spring Boot application with an optional Istio `VirtualService`.

## Install

```bash
helm upgrade --install mm2-offset-map ./charts/mm2-offset-map \
  --namespace mm2-offset-map \
  --create-namespace
```

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

For sensitive Kafka authentication values, create a secret containing `SPRING_APPLICATION_JSON` and reference it instead of putting credentials in values:

```bash
kubectl -n mm2-offset-map create secret generic mm2-offset-map-config \
  --from-literal=SPRING_APPLICATION_JSON='{"mm2":{"offset-map":{"bootstrap-servers":"target:9093","source-bootstrap-servers":"source:9093"}}}'
```

```yaml
configSecret:
  name: mm2-offset-map-config
  key: SPRING_APPLICATION_JSON
```

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
