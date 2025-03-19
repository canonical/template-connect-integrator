#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import random
import string

import pytest
from helpers import (
    CONNECT_APP,
    JDBC_CONNECTOR_DOWNLOAD_LINK,
    KAFKA_APP,
    PLUGIN_RESOURCE_KEY,
    POSTGRES_APP,
    DatabaseFixtureParams,
    assert_messages_produced,
    download_file,
    get_unit_ipv4_address,
    run_command_on_unit,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

SOURCE_DB = "source_db"
SINK_DB = "sink_db"

SOURCE_APP = "postgres-source-integrator"
SINK_APP = "postgres-sink-integrator"

FIXTURE_PARAMS = DatabaseFixtureParams(
    app_name=POSTGRES_APP, db_name=SOURCE_DB, no_tables=1, no_records=79
)


async def generate_test_data(ops_test: OpsTest, substrate: str, params: DatabaseFixtureParams):
    """Loads a postgres database with test data using the client shipped with postgresql charm.

    Tables are named table_{i}, i starting from 1 to param.no_tables.
    """
    leader = ops_test.model.applications[params.app_name].units[0]
    get_pass_action = await leader.run_action("get-password", mode="full", dryrun=False)
    response = await get_pass_action.wait()

    root_pass = response.results.get("password")
    postgres_host = await get_unit_ipv4_address(ops_test, leader)

    async def exec_query(query: str):
        query = query.replace("\n", " ")
        cmd = f'psql postgresql://operator:{root_pass}@{postgres_host}:5432/{params.db_name} -c "{query}"'
        # print a truncated output
        print(cmd.replace(root_pass, "******")[:1000])
        res = await run_command_on_unit(
            ops_test, leader, cmd, container=None if substrate == "vm" else "postgresql"
        )
        assert res.return_code == 0

    for i in range(1, params.no_tables + 1):

        await exec_query(
            f"""CREATE TABLE table_{i} (
            id serial not null primary key,
            name character varying(128) collate pg_catalog.default not null,
            price float default null,
            created_at timestamp with time zone not null default now()
        )"""
        )

        values = []
        for _ in range(params.no_records):
            random_name = "".join([random.choice(string.ascii_letters) for _ in range(8)])
            random_price = float(random.randint(10, 1000))
            values.append(f"('{random_name}', {random_price})")

        await exec_query(f"INSERT INTO table_{i} (name, price) VALUES {', '.join(values)}")


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_cluster(
    ops_test: OpsTest, deploy_kafka, deploy_kafka_connect, deploy_postgresql
):
    """Deploys kafka-connect charm along kafka (in KRaft mode)."""
    await ops_test.model.add_relation(CONNECT_APP, KAFKA_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[CONNECT_APP, KAFKA_APP, POSTGRES_APP], timeout=3000, status="active"
        )


@pytest.mark.abort_on_fail
async def test_deploy_app(ops_test: OpsTest, app_charm, tmp_path_factory):

    temp_dir = tmp_path_factory.mktemp("plugin")
    plugin_path = f"{temp_dir}/jdbc-plugin.tar"
    logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
    download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
    logging.info("Download finished successfully.")

    await ops_test.model.deploy(
        app_charm.charm,
        application_name=SOURCE_APP,
        resources={PLUGIN_RESOURCE_KEY: plugin_path, **app_charm.resources},
        config={"mode": "source", "db_name": SOURCE_DB},
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SOURCE_APP], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_source_integrator(ops_test: OpsTest):
    """Checks source integrator becomes active after related with postgres."""
    await ops_test.model.add_relation(SOURCE_APP, POSTGRES_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SOURCE_APP, POSTGRES_APP], idle_period=30, timeout=600
        )

    # should still be in blocked mode because it needs kafka-connect relation
    assert ops_test.model.applications[SOURCE_APP].status == "blocked"


@pytest.mark.abort_on_fail
async def test_relate_with_connect_starts_source_integrator(ops_test: OpsTest, substrate: str):
    """Checks source integrator task starts after relation with Kafka Connect."""
    await generate_test_data(ops_test, substrate, FIXTURE_PARAMS)

    logger.info(f"Loaded {FIXTURE_PARAMS.no_records} records into source postgres DB.")
    await ops_test.model.add_relation(SOURCE_APP, CONNECT_APP)

    logging.info("Sleeping...")
    async with ops_test.fast_forward(fast_interval="20s"):
        await ops_test.model.wait_for_idle(
            apps=[SOURCE_APP, CONNECT_APP], idle_period=60, timeout=600, status="active"
        )

    # test task is running
    assert "RUNNING" in ops_test.model.applications[SOURCE_APP].status_message


@pytest.mark.abort_on_fail
async def test_source_connector_pushed_to_kafka(ops_test: OpsTest, kafka_dns_resolver):

    await assert_messages_produced(
        ops_test, KAFKA_APP, topic="test_table_1", no_messages=FIXTURE_PARAMS.no_records
    )


@pytest.mark.abort_on_fail
async def test_deploy_sink_app(ops_test: OpsTest, app_charm, tmp_path_factory):

    temp_dir = tmp_path_factory.mktemp("plugin")
    plugin_path = f"{temp_dir}/jdbc-plugin.tar"
    logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
    download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
    logging.info("Download finished successfully.")

    await ops_test.model.deploy(
        app_charm.charm,
        application_name=SINK_APP,
        resources={PLUGIN_RESOURCE_KEY: plugin_path, **app_charm.resources},
        config={"mode": "sink", "db_name": SINK_DB},
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SINK_APP], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_sink_integrator(ops_test: OpsTest):

    await ops_test.model.add_relation(SINK_APP, POSTGRES_APP)
    await ops_test.model.add_relation(SINK_APP, CONNECT_APP)
    async with ops_test.fast_forward(fast_interval="30s"):
        await ops_test.model.wait_for_idle(
            apps=[SINK_APP, POSTGRES_APP, CONNECT_APP],
            idle_period=30,
            timeout=600,
            status="active",
        )
        await asyncio.sleep(120)

    assert "RUNNING" in ops_test.model.applications[SINK_APP].status_message


@pytest.mark.abort_on_fail
async def test_e2e_scenario(ops_test: OpsTest, substrate: str):

    leader = ops_test.model.applications[POSTGRES_APP].units[0]

    get_pass_action = await leader.run_action("get-password", mode="full", dryrun=False)
    response = await get_pass_action.wait()
    root_pass = response.results.get("password")
    host = await get_unit_ipv4_address(ops_test, leader)

    res = await run_command_on_unit(
        ops_test,
        leader,
        f"psql postgresql://operator:{root_pass}@{host}:5432/{SINK_DB} -c 'SELECT COUNT(*) FROM test_table_1'",
        container=None if substrate == "vm" else "postgresql",
    )

    logger.info(f"Checking number of records in sink DB (should be {FIXTURE_PARAMS.no_records}):")
    print(res.stdout)
    assert str(FIXTURE_PARAMS.no_records) in res.stdout
