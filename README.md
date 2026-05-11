# Template Charm for Kafka Connect Integrators

This simple template charm implements the requirer logic of the `connect_client` interface. Furthermore, in order to serve the connector plugins, this charm uses a FastAPI service exposed by default at port `8080` which serves the plugins by default at `api/v1/plugin`.

To build and use this charm, the `connect-plugin` resource should be provided either during deploy or build, with Charmhub being the preferred way.

## Build & Deploy Instructions

Example implementations are provided in the `examples` folder. You could easily build the integrator charm using `make` tool by providing appropriate build arguments:

- `NAME`: name of the integrator charm
- `BUILD_DIRECTORY`: path to the directory where charmcraft build artefacts are going to be stored
- `IMPL`: the target implementation, provided in the `examples` folder. Currently, valid values are: `mysql`, `opensearch`, `postgresql`, `s3`. If you would like a new implementation, you should implement the `BaseIntegrator` and `BaseConfigFormatter` interfaces, inspired by the examples provided.
- `DATA_INTERFACE`: type of the data_interface to use on the `data` relation, e.g. `mysql_client`, `kafka_client`, `s3`, etc.

### Example

```bash
make build \
    NAME=my-mysql-integrator \
    BUILD_DIRECTORY=build \
    IMPL=mysql \
    DATA_INTERFACE=mysql_client

juju deploy build/*.charm --resource connect-plugin=/path/to/plugin.tar --config mode=[source|sink] integrator
```

## Integration with Kafka Connect

```bash
# Deploy Apache Kafka & Kafka Connect charms
juju deploy kafka --config roles="broker,controller" -n 3
juju deploy kafka-connect 
juju integrate kafka kafka-connect

# Integrate kafka connect with the integrator
juju integrate kafka-connect integrator
```

## Interoperability matrix

### Direction Support

| Connector | Source | Sink | Plugin |
|-----------|:------:|:----:|--------|
| **MySQL** | Yes | Yes | Aiven JDBC v6.10.0 |
| **PostgreSQL** | Yes | Yes | Aiven JDBC v6.10.0 |
| **MongoDB** | Yes | Yes | Debezium v3.4.0 |
| **OpenSearch** | No | Yes | Aiven OpenSearch v3.1.1 |
| **S3** | No | Yes | Aiven S3 Sink v3.1.0 |
| **MirrorMaker** | N/A (replication) | N/A (replication) | Built-in (no plugin needed) |

### Data Format & Converter Requirements

| Connector | Key Converter | Value Converter | Format Constraint |
|-----------|--------------|-----------------|-------------------|
| **MySQL** | `StringConverter` | `JsonConverter` | Standard JSON |
| **PostgreSQL** | `StringConverter` | `JsonConverter` | Standard JSON |
| **MongoDB** | `StringConverter` | `JsonConverter` (schemas.enable=false) | Debezium CDC format. source emits change events (CREATE/UPDATE/DELETE); sink expects this format |
| **OpenSearch** | N/A (key.ignore=true) | JSON (schema.ignore=true) | Schemaless JSON |
| **S3** | `ByteArrayConverter` | `ByteArrayConverter` | Raw bytes (format-agnostic) |
| **MirrorMaker** | `ByteArrayConverter` | `ByteArrayConverter` | Raw bytes (passthrough) |

### Cross-Connector Sink/Source Compatibility

| Source ↓ / Sink → | MySQL | PostgreSQL | MongoDB | OpenSearch | S3 |
|--------------------|:-----:|:----------:|:-------:|:----------:|:--:|
| **MySQL** | Yes | Yes | No | Yes* | Yes |
| **PostgreSQL** | Yes | Yes | No | Yes* | Yes |
| **MongoDB** | No | No | Yes | No** | Yes |

\* **OpenSearch from JDBC sources**: Works because OpenSearch sink uses `schema.ignore=true` and `key.ignore=true`, so it can ingest any JSON payload. However, the JDBC source JSON structure may need the schema to be stripped (which `schema.ignore` handles).  
\*\* **OpenSearch from MongoDB**: Debezium CDC envelope format is nested JSON, OpenSearch would index the raw CDC envelope as-is. Technically possible but doesn't make sense.

### Notes

- **MySQL <--> PostgreSQL**: Both use the same Aiven JDBC connector with `JsonConverter`. They produce/consume compatible JSON, so they can cross-feed each other.
- **MongoDB sink requires Debezium CDC format**: The MongoDB sink expects Debezium-formatted change events. Only the MongoDB source produces this format, so **MongoDB sink only works with MongoDB source**.
- **MongoDB source produces Debezium CDC format**: The JSON emitted by the Debezium MongoDB source includes CDC envelope fields (`before`, `after`, `op`, `source`). This is **not compatible** with the JDBC sink connectors (MySQL/PostgreSQL) which expect flat JSON records.
- **S3 as universal sink**: Uses `ByteArrayConverter` for both key and value, so it accepts any format. Works with all sources.
