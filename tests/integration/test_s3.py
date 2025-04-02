#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import json
import logging
import os
import subprocess

import boto3
import pytest
from helpers import (
    CONNECT_APP,
    KAFKA_APP,
    PLUGIN_RESOURCE_KEY,
    DatabaseFixtureParams,
    download_file,
    get_plugin_url,
    produce_messages,
)
from pytest_operator.plugin import OpsTest
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "sinkbucket"
SINK_APP = "s3-sink-integrator"
S3_PROVIDER_CHARM = "s3-integrator"
S3_PROVIDER_APP = "s3-provider"


class S3EnvKeys:
    ACCESS_KEY = "S3_ACCESS_KEY"
    SECRET_KEY = "S3_SECRET_KEY"
    HOST = "S3_HOST"
    BUCKET = "S3_BUCKET"
    REGION = "S3_REGION"

    ALL = [ACCESS_KEY, SECRET_KEY, HOST, REGION, BUCKET]


FIXTURE_PARAMS = DatabaseFixtureParams(
    app_name=S3_PROVIDER_APP,
    db_name=os.environ.get(S3EnvKeys.BUCKET) or DEFAULT_BUCKET,
    no_records=161,
)


@retry(
    wait=wait_fixed(1),
    stop=stop_after_attempt(3),
    retry=retry_if_exception(lambda _: True),
)
def create_test_bucket(bucket: str):
    logger.info(f"Creating bucket {bucket}")
    client = boto3.client(
        "s3",
        endpoint_url=os.environ[S3EnvKeys.HOST],
        aws_access_key_id=os.environ[S3EnvKeys.ACCESS_KEY],
        aws_secret_access_key=os.environ[S3EnvKeys.SECRET_KEY],
    )
    client.create_bucket(Bucket=bucket)


@pytest.mark.abort_on_fail
def test_prepare_s3_env():
    """Either deploy microceph or use the provided S3 endpoint via env vars."""
    access_key = os.environ.get(S3EnvKeys.ACCESS_KEY)
    secret_key = os.environ.get(S3EnvKeys.SECRET_KEY)
    host = os.environ.get(S3EnvKeys.HOST)
    bucket = os.environ.get(S3EnvKeys.BUCKET)
    s3_params = [access_key, secret_key, host, bucket]

    if all(s3_params):
        logger.info("All required S3 params are set, skipping microceph installation.")
        return

    logger.warning(
        f"Attempting to install microceph since not all required env variables are set: {S3EnvKeys.ALL}"
    )

    subprocess.run(["sudo", "snap", "install", "microceph"], check=True)
    subprocess.run(["sudo", "microceph", "cluster", "bootstrap"], check=True)
    subprocess.run(["sudo", "microceph", "disk", "add", "loop,4G,3"], check=True)
    subprocess.run(["sudo", "microceph", "enable", "rgw"], check=True)
    output = subprocess.run(
        [
            "sudo",
            "microceph.radosgw-admin",
            "user",
            "create",
            "--uid",
            "test",
            "--display-name",
            "test",
        ],
        capture_output=True,
        check=True,
        encoding="utf-8",
    ).stdout
    key = json.loads(output)["keys"][0]
    access_key = key["access_key"]
    secret_key = key["secret_key"]

    # TODO: find a better solution?
    lxdbr_gateway = (
        subprocess.check_output(
            "lxc network show lxdbr0 | grep ipv4.address", shell=True, universal_newlines=True
        )
        .split(":")[-1]
        .strip()
        .split("/")[0]
    )

    logger.info("microceph setup finished successfully!")

    os.environ[S3EnvKeys.ACCESS_KEY] = access_key
    os.environ[S3EnvKeys.SECRET_KEY] = secret_key
    os.environ[S3EnvKeys.HOST] = f"http://{lxdbr_gateway}"

    logger.info(" ".join([f"{k}={os.environ.get(k)}" for k in S3EnvKeys.ALL]))


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_cluster(ops_test: OpsTest, deploy_kafka, deploy_kafka_connect):
    """Deploys kafka-connect charm along kafka (in KRaft mode) and s3-integrator."""
    os.environ[S3EnvKeys.BUCKET] = os.environ.get(S3EnvKeys.BUCKET) or DEFAULT_BUCKET
    create_test_bucket(os.environ[S3EnvKeys.BUCKET])

    await ops_test.model.deploy(
        S3_PROVIDER_CHARM,
        application_name=S3_PROVIDER_APP,
        num_units=1,
        series="jammy",
        config={
            "endpoint": os.environ[S3EnvKeys.HOST],
            "bucket": os.environ[S3EnvKeys.BUCKET],
            "region": os.environ.get(S3EnvKeys.REGION) or "us-east-1",
        },
    )

    await ops_test.model.add_relation(CONNECT_APP, KAFKA_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[CONNECT_APP, KAFKA_APP], timeout=3000, status="active"
        )

    s3_unit = ops_test.model.applications[S3_PROVIDER_APP].units[0]
    sync_creds_action = await s3_unit.run_action(
        "sync-s3-credentials",
        **{
            "mode": "full",
            "dryrun": False,
            "access-key": os.environ.get(S3EnvKeys.ACCESS_KEY),
            "secret-key": os.environ.get(S3EnvKeys.SECRET_KEY),
        },
    )
    _ = await sync_creds_action.wait()


@pytest.mark.abort_on_fail
async def test_produce_messages(ops_test: OpsTest, kafka_dns_resolver):
    """Since S3 does not have a source connector yet, fills a Kafka topic with data."""
    await produce_messages(ops_test, KAFKA_APP, FIXTURE_PARAMS.db_name, FIXTURE_PARAMS.no_records)

    logging.info(
        f"{FIXTURE_PARAMS.no_records} messages produced to topic {FIXTURE_PARAMS.db_name}"
    )


@pytest.mark.abort_on_fail
async def test_deploy_sink_app(ops_test: OpsTest, app_charm, tmp_path_factory):

    temp_dir = tmp_path_factory.mktemp("plugin")
    plugin_path = f"{temp_dir}/s3-plugin.tar"
    link = get_plugin_url()
    logging.info(f"Downloading S3 connectors from {link}...")
    download_file(link, plugin_path)
    logging.info("Download finished successfully.")

    bucket = os.environ[S3EnvKeys.BUCKET]

    await ops_test.model.deploy(
        app_charm.charm,
        application_name=SINK_APP,
        resources={PLUGIN_RESOURCE_KEY: plugin_path, **app_charm.resources},
        config={"bucket": bucket, "topics": FIXTURE_PARAMS.db_name},
    )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[SINK_APP], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_sink_integrator(ops_test: OpsTest):

    await ops_test.model.add_relation(SINK_APP, S3_PROVIDER_APP)
    await ops_test.model.add_relation(SINK_APP, CONNECT_APP)
    async with ops_test.fast_forward(fast_interval="30s"):
        await ops_test.model.wait_for_idle(
            apps=[SINK_APP, CONNECT_APP, S3_PROVIDER_APP],
            idle_period=60,
            timeout=600,
            status="active",
        )
        await asyncio.sleep(60)

    assert "RUNNING" in ops_test.model.applications[SINK_APP].status_message


@pytest.mark.abort_on_fail
async def test_e2e_scenario(ops_test: OpsTest):

    client = boto3.client(
        "s3",
        endpoint_url=os.environ[S3EnvKeys.HOST],
        aws_access_key_id=os.environ[S3EnvKeys.ACCESS_KEY],
        aws_secret_access_key=os.environ[S3EnvKeys.SECRET_KEY],
    )

    raw = client.list_objects(Bucket=os.environ[S3EnvKeys.BUCKET])
    objects = raw["Contents"]

    assert objects  # we should have some objects
    for obj in objects:
        # objects are named {topic}-##.gz
        assert FIXTURE_PARAMS.db_name in obj["Key"]
