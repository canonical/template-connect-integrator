#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


import subprocess

import pytest
from pytest_operator.plugin import OpsTest

KAFKA_CONNECT_REPO = "https://github.com/canonical/kafka-connect-operator.git"


@pytest.fixture(scope="module")
async def kafka_connect_charm(ops_test: OpsTest, tmp_path_factory):
    """Apache Kafka Connect charm.

    This fixture temporarily uses Apache Kafka Connect repository to build the charm,
    which should be replaced with the stable release of Kafka Connect.
    """
    tmp_dir = tmp_path_factory.mktemp("kafka-connect")
    clone_cmd = f"git clone {KAFKA_CONNECT_REPO}"
    subprocess.check_output(
        clone_cmd, stderr=subprocess.PIPE, universal_newlines=True, shell=True, cwd=tmp_dir
    )
    charm_path = f"{tmp_dir}/kafka-connect-operator/."
    charm = await ops_test.build_charm(charm_path)

    return charm


@pytest.fixture(scope="module")
async def app_charm(ops_test: OpsTest):
    """Build the source (MySQL) integrator charm."""
    charm_path = "./build/"
    charm = await ops_test.build_charm(charm_path)
    return charm
