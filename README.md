# Template Charm for Kafka Connect Integrators

This simple template charm implements the requirer logic of the `connect_client` interface. Furthermore, in order to serve the connector plugins, this charm uses a FastAPI service exposed by default at port `8080` which serves the plugins by default at `api/v1/plugin`.

To build and use this charm, the `connect-plugin` resource should be provided either during deploy or build, with Charmhub being the preferred way.

## Build & Deploy Instructions

Example implementations are provided in the `examples` folder. You could easily build the integrator charm using `make` tool by providing appropriate build aguments:

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
