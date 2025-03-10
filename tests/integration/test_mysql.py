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
    KAFKA_CHANNEL,
    MYSQL_APP,
    MYSQL_CHANNEL,
    PLUGIN_RESOURCE_KEY,
    DatabaseFixtureParams,
    assert_messages_produced,
    download_file,
    run_command_on_unit,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


SOURCE_DB = "source_db"
SINK_DB = "sink_db"

SOURCE_APP = "mysql-source-integrator"
SINK_APP = "mysql-sink-integrator"

FIXTURE_PARAMS = DatabaseFixtureParams(
    app_name="mysql", db_name=SOURCE_DB, no_tables=1, no_records=147
)


async def generate_test_data(ops_test: OpsTest, params: DatabaseFixtureParams):
    """Loads a MySQL database with test data using the client shipped with MySQL charm.

    Tables are named table_{i}, i starting from 1 to param.no_tables.
    """
    mysql_leader = ops_test.model.applications[params.app_name].units[0]
    get_pass_action = await mysql_leader.run_action("get-password", mode="full", dryrun=False)
    response = await get_pass_action.wait()

    mysql_root_pass = response.results.get("password")

    async def exec_query(query: str):
        query = query.replace("\n", " ")
        cmd = f'mysql -h 127.0.0.1 -u root -p{mysql_root_pass} -e "{query}"'
        # print a truncated output
        print(cmd.replace(mysql_root_pass, "******")[:1000])
        return_code, _, _ = await ops_test.juju("ssh", f"{mysql_leader.name}", cmd)
        # assert return_code == 0

    for i in range(1, params.no_tables + 1):

        await exec_query(
            f"""CREATE TABLE {params.db_name}.table_{i} (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(50) DEFAULT NULL,
            price float DEFAULT NULL,
            created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY product_id_uindex (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""
        )

        values = []
        for _ in range(params.no_records):
            random_name = "".join([random.choice(string.ascii_letters) for _ in range(8)])
            random_price = float(random.randint(10, 1000))
            values.append(f"('{random_name}', {random_price})")

        await exec_query(
            f"INSERT INTO {params.db_name}.table_{i} (name, price) Values {', '.join(values)}"
        )


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
    )

    await ops_test.model.add_relation(CONNECT_APP, KAFKA_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[CONNECT_APP, KAFKA_APP, MYSQL_APP], timeout=3000, status="active"
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
        application_name=SOURCE_APP,
        resources={PLUGIN_RESOURCE_KEY: plugin_path},
        config={"mode": "source", "db_name": SOURCE_DB},
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SOURCE_APP], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_source_integrator(ops_test: OpsTest):
    """Checks source integrator becomes active after related with MySQL."""
    # our source mysql integrator need a mysql_client relation to unblock:
    await ops_test.model.add_relation(SOURCE_APP, MYSQL_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SOURCE_APP, MYSQL_APP], idle_period=30, timeout=600
        )

    # should still be in blocked mode because it needs kafka-connect relation
    assert ops_test.model.applications[SOURCE_APP].status == "blocked"


@pytest.mark.abort_on_fail
async def test_relate_with_connect_starts_source_integrator(ops_test: OpsTest):
    """Checks source integrator task starts after relation with Kafka Connect."""
    await generate_test_data(ops_test, FIXTURE_PARAMS)

    logger.info(f"Loaded {FIXTURE_PARAMS.no_records} records into source MySQL DB.")
    await ops_test.model.add_relation(SOURCE_APP, CONNECT_APP)

    logging.info("Sleeping...")
    async with ops_test.fast_forward(fast_interval="20s"):
        await asyncio.sleep(180)

    # test task is running
    assert ops_test.model.applications[SOURCE_APP].status == "active"
    assert "RUNNING" in ops_test.model.applications[SOURCE_APP].status_message


@pytest.mark.abort_on_fail
async def test_source_connector_pushed_to_kafka(ops_test: OpsTest):

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
        app_charm,
        application_name=SINK_APP,
        resources={PLUGIN_RESOURCE_KEY: plugin_path},
        config={"mode": "sink", "db_name": SINK_DB},
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SINK_APP], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_sink_integrator(ops_test: OpsTest):

    await ops_test.model.add_relation(SINK_APP, MYSQL_APP)
    await ops_test.model.add_relation(SINK_APP, CONNECT_APP)
    async with ops_test.fast_forward(fast_interval="30s"):
        await ops_test.model.wait_for_idle(
            apps=[SINK_APP, MYSQL_APP, CONNECT_APP], idle_period=30, timeout=600
        )
        await asyncio.sleep(120)

    # should still be in blocked mode because it needs kafka-connect relation
    assert ops_test.model.applications[SINK_APP].status == "active"
    assert "RUNNING" in ops_test.model.applications[SINK_APP].status_message


@pytest.mark.abort_on_fail
async def test_e2e_scenario(ops_test: OpsTest):

    leader = ops_test.model.applications[MYSQL_APP].units[0]

    get_pass_action = await leader.run_action("get-password", mode="full", dryrun=False)
    response = await get_pass_action.wait()
    root_pass = response.results.get("password")

    res = await run_command_on_unit(
        ops_test,
        leader,
        f"mysql -h 127.0.0.1 -u root -p{root_pass} -e 'SELECT COUNT(*) FROM {SINK_DB}.test_table_1'",
    )

    logger.info(f"Checking number of records in sink DB (should be {FIXTURE_PARAMS.no_records}):")
    print(res.stdout)
    assert str(FIXTURE_PARAMS.no_records) in res.stdout
