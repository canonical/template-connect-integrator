#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging

import pytest
from helpers import (
    CONNECT_APP,
    JDBC_CONNECTOR_DOWNLOAD_LINK,
    KAFKA_APP,
    KAFKA_CHANNEL,
    MYSQL_APP,
    MYSQL_CHANNEL,
    MYSQL_DB,
    MYSQL_INTEGRATOR,
    PLUGIN_RESOURCE_KEY,
    POSTGRES_APP,
    POSTGRES_CHANNEL,
    DatabaseFixtureParams,
    download_file,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_cluster(ops_test: OpsTest, kafka_connect_charm):
    """Deploys kafka-connect charm along kafka (in KRaft mode)."""
    await asyncio.gather(
        ops_test.model.deploy(
            kafka_connect_charm,
            application_name=CONNECT_APP,
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            KAFKA_APP,
            channel=KAFKA_CHANNEL,
            application_name=KAFKA_APP,
            num_units=1,
            series="jammy",
            config={"roles": "broker,controller"},
        ),
        ops_test.model.deploy(
            MYSQL_APP,
            channel=MYSQL_CHANNEL,
            application_name=MYSQL_APP,
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            POSTGRES_APP,
            channel=POSTGRES_CHANNEL,
            application_name=POSTGRES_APP,
            num_units=1,
            series="jammy",
        ),
    )

    await ops_test.model.add_relation(CONNECT_APP, KAFKA_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[CONNECT_APP, KAFKA_APP, MYSQL_APP, POSTGRES_APP], timeout=3000, status="active"
        )


@pytest.mark.abort_on_fail
async def test_deploy_app(ops_test: OpsTest, app_charm, tmp_path_factory):

    temp_dir = tmp_path_factory.mktemp("plugin")
    plugin_path = f"{temp_dir}/jdbc-plugin.tar"
    logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
    download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
    logging.info("Download finished successfully.")

    await ops_test.model.deploy(
        app_charm,
        application_name=MYSQL_INTEGRATOR,
        resources={PLUGIN_RESOURCE_KEY: plugin_path},
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_source_integrator(ops_test: OpsTest):
    """Checks source integrator becomes active after related with MySQL."""
    # our source mysql integrator need a mysql_client relation to unblock:
    await ops_test.model.add_relation(MYSQL_INTEGRATOR, MYSQL_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR, MYSQL_APP], idle_period=30, timeout=600
        )

    assert ops_test.model.applications[MYSQL_INTEGRATOR].status == "active"
    assert "UNASSIGNED" in ops_test.model.applications[MYSQL_INTEGRATOR].status_message


@pytest.mark.abort_on_fail
@pytest.mark.parametrize(
    "mysql_test_data",
    [DatabaseFixtureParams(app_name="mysql", db_name=MYSQL_DB, no_tables=1, no_records=20)],
    indirect=True,
)
async def test_relate_with_connect_starts_source_integrator(ops_test: OpsTest, mysql_test_data):
    """Checks source integrator task starts after relation with Kafka Connect."""
    # Hopefully, mysql_test_data fixture has filled our db with some test data.
    # Now it's time relate to Kafka Connect to start the task.
    logger.info("Loaded 20 records into source MySQL DB.")
    await ops_test.model.add_relation(MYSQL_INTEGRATOR, CONNECT_APP)

    assert ops_test.model.applications[MYSQL_INTEGRATOR].status == "active"

    logging.info("Sleeping for a minute...")
    async with ops_test.fast_forward(fast_interval="20s"):
        await asyncio.sleep(60)

    # test task is running
    assert "RUNNING" in ops_test.model.applications[MYSQL_INTEGRATOR].status_message
