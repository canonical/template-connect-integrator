#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging

import pytest
from helpers import (
    CONNECT_APP,
    KAFKA_APP,
    KAFKA_APP_B,
    assert_messages_produced,
    produce_messages,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

MM_APP = "mirrormaker"
PRODUCER_APP = "producer"


# Deployment with KAFKA A and KAFKA B. These two charms will be used to test active-passive and
# active-active replication. Kafka A will also be used as connect backend.


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_cluster(
    ops_test: OpsTest,
    deploy_kafka,
    deploy_kafka_connect,
    deploy_kafka_passive,
):
    """Deploys kafka-connect charm along kafka (in KRaft mode)."""
    await ops_test.model.integrate(CONNECT_APP, KAFKA_APP_B)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[CONNECT_APP, KAFKA_APP, KAFKA_APP_B], timeout=3000, status="active"
        )


@pytest.mark.abort_on_fail
async def test_deploy_app(ops_test: OpsTest, app_charm):
    """Deploys active-passive scenario."""
    mm_config = {"prefix_topics": "false"}
    await ops_test.model.deploy(
        app_charm.charm,
        application_name=MM_APP,
        resources={**app_charm.resources},
        config=mm_config,
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MM_APP], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_integrator(ops_test: OpsTest):
    """Checks integrator becomes active after related with active and passive ends of Kafka."""
    await ops_test.model.integrate(f"{MM_APP}:source", KAFKA_APP)
    await ops_test.model.integrate(f"{MM_APP}:target", KAFKA_APP_B)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MM_APP, KAFKA_APP, KAFKA_APP_B], idle_period=30, timeout=600
        )

    # should still be in blocked mode because it needs kafka-connect relation
    assert ops_test.model.applications[MM_APP].status == "blocked"


@pytest.mark.abort_on_fail
async def test_relate_with_connect_starts_integrator(ops_test: OpsTest):
    """Checks source integrator task starts after relation with Kafka Connect."""
    await produce_messages(ops_test, KAFKA_APP, topic="arnor", no_messages=100)
    logging.info("100 messages produced to topic arnor")

    await ops_test.model.integrate(MM_APP, CONNECT_APP)

    logging.info("Sleeping...")
    async with ops_test.fast_forward(fast_interval="20s"):
        await ops_test.model.wait_for_idle(
            apps=[MM_APP, CONNECT_APP], idle_period=60, timeout=600, status="active"
        )

    # test task is running
    assert "RUNNING" in ops_test.model.applications[MM_APP].status_message


@pytest.mark.abort_on_fail
async def test_consume_messages_on_passive_cluster(ops_test: OpsTest):
    """Produce messages to a Kafka topic."""
    # Give some time for the messages to be replicated
    await asyncio.sleep(120)

    # Check that the messages can be consumed on the passive cluster
    await assert_messages_produced(ops_test, KAFKA_APP_B, topic="arnor", no_messages=100)


@pytest.mark.abort_on_fail
async def test_consumer_groups(ops_test: OpsTest):
    """Produce messages to a Kafka topic, and check that the consumer groups are replicated."""
    # Consume messages on the active cluster, using the consumer group this time
    await assert_messages_produced(
        ops_test, KAFKA_APP, topic="arnor", no_messages=100, consumer_group="arnor-1"
    )

    # Produce some more messages
    await produce_messages(ops_test, KAFKA_APP_B, topic="arnor", no_messages=40)

    # Give some time for the messages to be replicated
    await asyncio.sleep(40)

    # Consumed messages on the passive cluster should only be the new ones,
    # using the same consumer group
    await assert_messages_produced(
        ops_test, KAFKA_APP_B, topic="arnor", no_messages=40, consumer_group="arnor-1"
    )
