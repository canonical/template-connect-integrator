#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


import dataclasses
import os
from functools import partial
from pathlib import Path

import pytest
from helpers import (
    CONNECT_APP,
    CONNECT_CHANNEL,
    K8S_IMAGE,
    KAFKA_APP,
    KAFKA_CHANNEL,
    MYSQL_APP,
    MYSQL_CHANNEL,
    POSTGRES_APP,
    POSTGRES_CHANNEL,
    get_unit_ipv4_address,
    patched_dns_lookup,
)
from pytest_operator.plugin import OpsTest


@dataclasses.dataclass
class AppCharm:
    charm: Path
    resources: dict


@pytest.fixture(scope="module")
def substrate() -> str:
    """Test Substrate which is read from `SUBSTRATE` env var."""
    _env = os.environ.get("SUBSTRATE", "vm").lower()

    if _env not in ("vm", "k8s"):
        raise Exception(f"Unknown Substrate: {_env}")

    return _env


@pytest.fixture(scope="module")
async def deploy_kafka_connect(ops_test: OpsTest, substrate) -> None:
    """Deploys the Apache Kafka Connect charm, the deployed application name would be determined by `CONNECT_APP` variable."""
    charm_name = "kafka-connect" if substrate == "vm" else "kafka-connect-k8s"
    await ops_test.model.deploy(
        charm_name,
        channel=CONNECT_CHANNEL,
        application_name=CONNECT_APP,
        num_units=1,
    )


@pytest.fixture(scope="module")
async def app_charm(ops_test: OpsTest, substrate: str) -> AppCharm:
    """Builds the integrator charm."""
    charm_path = "./build/"
    charm = await ops_test.build_charm(charm_path)

    resources = {} if substrate == "vm" else {"base-image": K8S_IMAGE}

    return AppCharm(charm=charm, resources=resources)


@pytest.fixture(scope="module")
async def deploy_kafka(ops_test: OpsTest, substrate) -> None:
    """Deploys the Apache Kafka Charm, the deployed application name would be determined by `KAFKA_APP` variable."""
    charm_name = "kafka" if substrate == "vm" else "kafka-k8s"
    await ops_test.model.deploy(
        charm_name,
        channel=KAFKA_CHANNEL,
        application_name=KAFKA_APP,
        num_units=1,
        config={"roles": "broker,controller"},
    )


@pytest.fixture(scope="module")
async def deploy_mysql(ops_test: OpsTest, substrate) -> None:
    """Deploys the MySQL Charm, the deployed application name would be determined by `MYSQL_APP` variable."""
    charm_name = "mysql" if substrate == "vm" else "mysql-k8s"
    kwargs = {} if substrate == "vm" else {"trust": True}
    await ops_test.model.deploy(
        charm_name, channel=MYSQL_CHANNEL, application_name=MYSQL_APP, num_units=1, **kwargs
    )


@pytest.fixture(scope="module")
async def deploy_postgresql(ops_test: OpsTest, substrate) -> None:
    """Deploys the PostgreSQL Charm, the deployed application name would be determined by `POSTGRES_APP` variable."""
    charm_name = "postgresql" if substrate == "vm" else "postgresql-k8s"
    kwargs = {} if substrate == "vm" else {"trust": True}
    await ops_test.model.deploy(
        charm_name, channel=POSTGRES_CHANNEL, application_name=POSTGRES_APP, num_units=1, **kwargs
    )


@pytest.fixture(scope="function")
async def kafka_dns_resolver(ops_test: OpsTest, substrate, monkeypatch):
    """Fixture which patches DNS lookup for `kafka` client library so that it works properly in K8s environment."""
    if substrate == "vm":
        yield
        return

    records = {}
    for app in ops_test.model.applications.values():
        for unit in app.units:
            unit_ip = await get_unit_ipv4_address(ops_test, unit)
            parts = unit.name.split("/")
            records[f"{app.name}-{parts[1]}.{app.name}-endpoints"] = unit_ip

    print(records)

    monkeypatch.setattr("kafka.conn.dns_lookup", partial(patched_dns_lookup, records=records))
    yield

    monkeypatch.undo()
