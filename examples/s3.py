#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for S3 sink."""


from charms.data_platform_libs.v0.s3 import S3Requirer
from charms.kafka_connect.v0.integrator import BaseConfigFormatter, BaseIntegrator, ConfigOption
from typing_extensions import override

from workload import PluginServer


class S3ConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Aiven S3 sink connector configuration."""

    # configurable options
    topics = ConfigOption(
        json_key="topics", default="test", description="Topics to read data from."
    )

    # non-configurable options
    connector_class = ConfigOption(
        json_key="connector.class",
        default="io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
        configurable=False,
    )
    key_converter = ConfigOption(
        json_key="key.converter",
        default="org.apache.kafka.connect.converters.ByteArrayConverter",
        configurable=False,
    )
    value_converter = ConfigOption(
        json_key="value.converter",
        default="org.apache.kafka.connect.converters.ByteArrayConverter",
        configurable=False,
    )
    tasks_max = ConfigOption(json_key="tasks.max", default=1, configurable=False)

    # general charm config
    bucket = ConfigOption(json_key="na", default="test", mode="none")
    mode = ConfigOption(
        json_key="na",
        default="sink",
        configurable=False,
        mode="none",
    )


class Integrator(BaseIntegrator):
    """Basic implementation for Kafka Connect S3 Sink Integrator."""

    name = "s3-sink-integrator"
    formatter = S3ConfigFormatter
    plugin_server = PluginServer

    CLIENT_REL = "data"
    CONNECT_REL = "connect-client"

    def __init__(self, /, charm, plugin_server_args=[], plugin_server_kwargs={}):
        super().__init__(charm, plugin_server_args, plugin_server_kwargs)

        self.name = charm.app.name
        self.bucket_name = str(self.charm.config.get("bucket", "test"))

        self.s3 = S3Requirer(charm, self.CLIENT_REL, self.bucket_name)

    @override
    def setup(self) -> None:
        data = self.s3.get_s3_connection_info()
        self.configure(
            {
                "aws.access.key.id": data["access-key"],
                "aws.secret.access.key": data["secret-key"],
                "aws.s3.bucket.name": data["bucket"],
                "aws.s3.region": data["region"],
                "aws.s3.endpoint": data["endpoint"],
            }
        )

    @override
    def teardown(self):
        pass

    @property
    @override
    def ready(self):
        return all(
            {
                self.helpers.check_data_interfaces_ready([self.CONNECT_REL]),
                self.helpers.check_data_interfaces_ready(
                    [self.CLIENT_REL], check_for=["access-key", "secret-key", "endpoint", "bucket"]
                ),
            }
        )
