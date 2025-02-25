#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


import random
import string
import subprocess
from typing import cast

import pytest
from helpers import DatabaseFixtureParams
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


@pytest.fixture(scope="function")
async def mysql_test_data(ops_test: OpsTest, request: pytest.FixtureRequest):
    """Loads a MySQL database with test data using the client shipped with MySQL charm.

    Tables are named table_{i}, i starting from 1 to param.no_tables.
    """
    params = cast(DatabaseFixtureParams, request.param)

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
