#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import random
import string

import pytest
from helpers import (
    CONNECT_APP,
    KAFKA_APP,
    MONGODB_APP,
    PLUGIN_RESOURCE_KEY,
    assert_messages_produced,
    download_file,
    get_plugin_url,
    get_secret_data,
    get_unit_ipv4_address,
    run_command_on_unit,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


SOURCE_DB = "test_db"
SOURCE_APP = "mongodb-source-integrator"
NO_RECORDS = 50


async def get_relation_credentials(ops_test: OpsTest) -> tuple[str, str]:
    """Gets the relation user credentials from Juju secrets.

    Returns:
        Tuple of (username, password) for the relation user.
    """
    data = await get_secret_data(ops_test, r"database\.[0-9]+\.user\.secret")
    return data["username"], data["password"]


async def grant_cluster_monitor(ops_test: OpsTest, substrate: str, username: str, password: str):
    """Grants clusterMonitor role to the relation user via the operator user.

    Debezium requires clusterMonitor to read config.shards for sharded cluster detection.
    """
    mongo_unit = ops_test.model.applications[MONGODB_APP].units[0]
    mongo_ip = await get_unit_ipv4_address(ops_test, mongo_unit)

    grant_cmd = f'db.grantRolesToUser("{username}", ["clusterMonitor"])'
    mongosh_cmd = [
        "charmed-mongodb.mongosh" if substrate == "vm" else "mongosh",
        f'"mongodb://{username}:{password}@{mongo_ip}:27017/admin?replicaSet=mongodb&authSource=admin"',
        "--eval",
        f"'{grant_cmd}'",
    ]

    if substrate == "vm":
        res = await run_command_on_unit(ops_test, mongo_unit, mongosh_cmd)
    else:
        res = await run_command_on_unit(ops_test, mongo_unit, mongosh_cmd, container="mongod")

    logger.info(f"Grant clusterMonitor result: {res.stdout}")
    assert res.return_code == 0


async def generate_test_data(
    ops_test: OpsTest, substrate: str, username: str, password: str, db_name: str, no_records: int
):
    """Loads a MongoDB database with test data using the relation user."""
    mongo_unit = ops_test.model.applications[MONGODB_APP].units[0]
    mongo_ip = await get_unit_ipv4_address(ops_test, mongo_unit)

    documents = []
    for _ in range(no_records):
        random_name = "".join([random.choice(string.ascii_letters) for _ in range(8)])
        random_price = float(random.randint(10, 1000))
        documents.append(f'{{name: "{random_name}", price: {random_price}}}')

    insert_cmd = f"db.test_collection.insertMany([{', '.join(documents)}])"
    mongosh_cmd = [
        "charmed-mongodb.mongosh" if substrate == "vm" else "mongosh",
        f'"mongodb://{username}:{password}@{mongo_ip}:27017/{db_name}?replicaSet=mongodb&authSource=admin"',
        "--eval",
        f"'{insert_cmd}'",
    ]

    if substrate == "vm":
        res = await run_command_on_unit(ops_test, mongo_unit, mongosh_cmd)
    else:
        res = await run_command_on_unit(ops_test, mongo_unit, mongosh_cmd, container="mongod")

    logger.info(f"Insert result: {res.stdout}")
    assert res.return_code == 0


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_cluster(
    ops_test: OpsTest, deploy_kafka, deploy_kafka_connect, deploy_mongodb
):
    """Deploys kafka-connect charm along kafka (in KRaft mode) and mongodb."""
    await ops_test.model.add_relation(CONNECT_APP, KAFKA_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[CONNECT_APP, KAFKA_APP, MONGODB_APP], timeout=3000, status="active"
        )


@pytest.mark.abort_on_fail
async def test_deploy_app(ops_test: OpsTest, app_charm, tmp_path_factory):
    """Deploys the MongoDB source integrator charm."""
    temp_dir = tmp_path_factory.mktemp("plugin")
    plugin_path = f"{temp_dir}/debezium-mongodb-plugin.tar"
    link = get_plugin_url()
    logging.info(f"Downloading Debezium MongoDB connector from {link}...")
    download_file(link, plugin_path)
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
    """Checks source integrator becomes active after related with MongoDB."""
    await ops_test.model.add_relation(SOURCE_APP, MONGODB_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SOURCE_APP, MONGODB_APP], idle_period=30, timeout=600
        )

    # should still be in blocked mode because it needs kafka-connect relation
    assert ops_test.model.applications[SOURCE_APP].status == "blocked"


@pytest.mark.abort_on_fail
async def test_grant_cluster_monitor_and_generate_data(ops_test: OpsTest, substrate: str):
    """Grants clusterMonitor role to the relation user and inserts test data."""
    username, password = await get_relation_credentials(ops_test)
    await grant_cluster_monitor(ops_test, substrate, username, password)
    await generate_test_data(ops_test, substrate, username, password, SOURCE_DB, NO_RECORDS)
    logger.info(f"Loaded {NO_RECORDS} records into source MongoDB.")


@pytest.mark.abort_on_fail
async def test_relate_with_connect_starts_source_integrator(ops_test: OpsTest):
    """Checks source integrator task starts after relation with Kafka Connect."""
    await ops_test.model.add_relation(SOURCE_APP, CONNECT_APP)

    logging.info("Waiting for connector to start...")
    async with ops_test.fast_forward(fast_interval="20s"):
        await ops_test.model.wait_for_idle(
            apps=[SOURCE_APP, CONNECT_APP], idle_period=60, timeout=600, status="active"
        )

    # test task is running
    assert "RUNNING" in ops_test.model.applications[SOURCE_APP].status_message


@pytest.mark.abort_on_fail
async def test_source_connector_pushed_to_kafka(ops_test: OpsTest, kafka_dns_resolver):
    """Checks that Debezium CDC events from MongoDB are produced to Kafka."""
    # source creates topics with pattern: <topic.prefix>.<db>.<collection>
    await assert_messages_produced(
        ops_test, KAFKA_APP, topic=f"test_.{SOURCE_DB}.test_collection", no_messages=NO_RECORDS
    )
