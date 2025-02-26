# Template Charm for Kafka Connect Integrators

This simple template charm implements the requirer logic of the `connect_client` interface. Furthermore, in order to serve the connector plugins, this charm uses a FastAPI service exposed by default at port `8080` which serves the plugins by default at `api/v1/plugin`.

To build and use this charm, the `connect-plugin` resource should be provided either during deploy or build, with Charmhub being the preferred way.

## Build & Deploy Instructions

To use this template charm, you should provide an implementation of `BaseIntegrator` and `BaseConfigFormatter`. Some example implementations are provided in the `examples` folder.  Then, you could easily build the integrator charm using `make` tool:

```bash
make build \
    NAME=my-mysql-integrator \
    BUILD_DIRECTORY=build \
    IMPL=examples.mysql \
    DATA_INTERFACE=mysql_client

juju deploy build/*.charm --resource connect-plugin=/path/to/plugin.tar --config mode=[source|sink]
```

## Integration with Kafka Connect

```bash
# Deploy Apache Kafka & Kafka Connect charms
juju deploy kafka --config roles="broker,controller" -n 3
juju deploy kafka-connect 
juju integrate kafka kafka-connect

# Integrate kafka connect with the integrator
juju integrate kafka-connect kafka-connect-integrator
```
