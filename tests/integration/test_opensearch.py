#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import os

import pytest
import requests
from helpers import (
    CONNECT_APP,
    KAFKA_APP,
    KAFKA_CHANNEL,
    OPENSEARCH_CONNECTOR_LINK,
    PLUGIN_RESOURCE_KEY,
    DatabaseFixtureParams,
    download_file,
    get_secret_data,
    get_unit_ipv4_address,
    produce_messages,
)
from pytest_operator.plugin import OpsTest
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

OPENSEARCH_APP = "opensearch"
OPENSEARCH_CHANNEL = "2/edge"
TLS_APP = "self-signed-certificates"
TLS_CHANNEL = "latest/stable"
TLS_REV = 155
INDEX = "sink_index"
SINK_APP = "os-sink-integrator"
FIXTURE_PARAMS = DatabaseFixtureParams(app_name=OPENSEARCH_APP, db_name=INDEX, no_records=53)

MODEL_CONFIG = {
    "update-status-hook-interval": "5m",
    "cloudinit-userdata": """postruncmd:
        - [ 'sysctl', '-w', 'vm.max_map_count=262144' ]
        - [ 'sysctl', '-w', 'fs.file-max=1048576' ]
        - [ 'sysctl', '-w', 'vm.swappiness=0' ]
        - [ 'sysctl', '-w', 'net.ipv4.tcp_retries2=5' ]
    """,
}

SYSCTL_CONFIG = [
    "vm.max_map_count=262144",
    "vm.swappiness=0",
    "net.ipv4.tcp_retries2=5",
    "fs.file-max=1048576",
]


def config_sysctl_params():
    for config in SYSCTL_CONFIG:
        ret = os.system(f'echo "{config}" | sudo tee -a /etc/sysctl.conf')
        assert not ret

    assert not os.system("sudo sysctl -p")


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_cluster(ops_test: OpsTest, deploy_kafka_connect):
    """Deploys kafka-connect charm along kafka (in KRaft mode) and Opensearch."""
    if "CI" in os.environ:
        config_sysctl_params()

    await ops_test.model.set_config(MODEL_CONFIG)
    await asyncio.gather(
        ops_test.model.deploy(
            KAFKA_APP,
            channel=KAFKA_CHANNEL,
            application_name=KAFKA_APP,
            num_units=1,
            series="jammy",
            config={"roles": "broker,controller"},
        ),
        ops_test.model.deploy(
            OPENSEARCH_APP,
            channel=OPENSEARCH_CHANNEL,
            application_name=OPENSEARCH_APP,
            config={"profile": "testing"},
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            TLS_APP,
            channel=TLS_CHANNEL,
            revision=TLS_REV,
            application_name=TLS_APP,
            series="jammy",
        ),
    )

    await ops_test.model.add_relation(CONNECT_APP, KAFKA_APP)
    await ops_test.model.add_relation(CONNECT_APP, TLS_APP)
    await ops_test.model.add_relation(OPENSEARCH_APP, TLS_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[CONNECT_APP, KAFKA_APP, OPENSEARCH_APP], timeout=3000, status="active"
        )


@pytest.mark.abort_on_fail
async def test_disable_disk_threshold_enabled(ops_test: OpsTest):
    """Disables the `disk.threshold_enabled` config on Opensearch which could cause index creation blocked errors in CI."""
    leader = ops_test.model.applications[OPENSEARCH_APP].units[0]

    get_pass_action = await leader.run_action("get-password", mode="full", dryrun=False)
    response = await get_pass_action.wait()
    admin_pass = response.results.get("password")

    os_ip = await get_unit_ipv4_address(ops_test, leader)

    resp = requests.put(
        f"https://{os_ip}:9200/_cluster/settings",
        auth=HTTPBasicAuth("admin", admin_pass),
        verify=False,
        json={
            "persistent": {
                "cluster.routing.allocation.disk.threshold_enabled": "false",
            }
        },
    )

    assert resp.status_code == 200
    assert resp.json().get("acknowledged", False)


@pytest.mark.abort_on_fail
async def test_produce_messages(ops_test: OpsTest):
    """Since Opensearch does not have a source connector yet, fills a Kafka topic with data."""
    await produce_messages(ops_test, KAFKA_APP, FIXTURE_PARAMS.db_name, FIXTURE_PARAMS.no_records)

    logging.info(
        f"{FIXTURE_PARAMS.no_records} messages produced to topic {FIXTURE_PARAMS.db_name}"
    )


@pytest.mark.abort_on_fail
async def test_deploy_sink_app(ops_test: OpsTest, app_charm, tmp_path_factory):

    temp_dir = tmp_path_factory.mktemp("plugin")
    plugin_path = f"{temp_dir}/os-plugin.tar"
    logging.info(f"Downloading Opensearch connectors from {OPENSEARCH_CONNECTOR_LINK}...")
    download_file(OPENSEARCH_CONNECTOR_LINK, plugin_path)
    logging.info("Download finished successfully.")

    await ops_test.model.deploy(
        app_charm.charm,
        application_name=SINK_APP,
        resources={PLUGIN_RESOURCE_KEY: plugin_path},
        config={"mode": "sink", "index_name": INDEX, "topics": INDEX},
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SINK_APP], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_sink_integrator(ops_test: OpsTest):

    await ops_test.model.add_relation(SINK_APP, OPENSEARCH_APP)
    await ops_test.model.add_relation(SINK_APP, CONNECT_APP)
    async with ops_test.fast_forward(fast_interval="30s"):
        await ops_test.model.wait_for_idle(
            apps=[SINK_APP, CONNECT_APP], idle_period=30, timeout=600
        )
        await asyncio.sleep(180)

    assert ops_test.model.applications[SINK_APP].status == "active"
    assert "RUNNING" in ops_test.model.applications[SINK_APP].status_message


@pytest.mark.abort_on_fail
async def test_e2e_scenario(ops_test: OpsTest):

    data = await get_secret_data(ops_test, r"opensearch-client\.[0-9]+\.user\.secret")
    username = data["username"]
    password = data["password"]

    os_unit = ops_test.model.applications[OPENSEARCH_APP].units[0]
    os_ip = await get_unit_ipv4_address(ops_test, os_unit)

    resp = requests.get(
        f"https://{os_ip}:9200/{INDEX}/_search",
        auth=HTTPBasicAuth(username, password),
        verify=False,
    )

    logger.info(
        f"Checking number of records in sink index (should be {FIXTURE_PARAMS.no_records}):"
    )

    print(resp.json())
    assert (
        resp.json().get("hits", {}).get("total", {}).get("value", 0) == FIXTURE_PARAMS.no_records
    )
