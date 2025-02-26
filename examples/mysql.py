#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for MySQL sources."""


from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequirerData,
    DatabaseRequirerEventHandlers,
)
from charms.kafka_connect.v0.integrator import BaseConfigFormatter, BaseIntegrator, ConfigOption
from typing_extensions import override

from workload import PluginServer


class MySQLConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Aiven JDBC Source connector configuration."""

    # configurable options
    topic_prefix = ConfigOption(json_key="topic.prefix", default="test_")
    select_mode = ConfigOption(json_key="mode", default="incrementing")
    incrementing_column = ConfigOption(json_key="incrementing.column.name", default="id")
    topic_partitions = ConfigOption(
        json_key="topic.creation.default.partitions", default=10
    )
    topic_replication_factor = ConfigOption(
        json_key="topic.creation.default.replication.factor", default=-1
    )
    db_name = ConfigOption(json_key="ignore.db_name", default="test_db")

    # non-configurable options
    connector_class = ConfigOption(
        json_key="connector.class",
        default="io.aiven.connect.jdbc.JdbcSourceConnector",
        configurable=False,
    )
    tasks_max = ConfigOption(json_key="tasks.max", default=1, configurable=False)
    auto_create = ConfigOption(json_key="auto.create", default=True, configurable=False)
    auto_evolve = ConfigOption(json_key="auto.evolve", default=True, configurable=False)
    value_converter = ConfigOption(
        json_key="value.converter",
        default="org.apache.kafka.connect.json.JsonConverter",
        configurable=False,
    )


class Integrator(BaseIntegrator):
    """Basic implementation for Kafka Connect MySQL Source Integrator."""

    name = "mysql-source-integrator"
    formatter = MySQLConfigFormatter
    plugin_server = PluginServer

    CONNECT_REL = "connect-client"
    DB_CLIENT_REL = "data"

    def __init__(self, /, charm, plugin_server_args=[], plugin_server_kwargs={}):
        super().__init__(charm, plugin_server_args, plugin_server_kwargs)

        self.db_name = str(self.charm.config.get("db_name", "test_db"))

        self.database_requirer_data = DatabaseRequirerData(
            self.model, self.DB_CLIENT_REL, self.db_name, extra_user_roles="admin"
        )
        self.database = DatabaseRequirerEventHandlers(self.charm, self.database_requirer_data)

    @override
    def setup(self) -> None:
        db = self.helpers.fetch_all_relation_data(self.DB_CLIENT_REL)
        self.configure(
            {
                "connection.url": f"jdbc:mysql://{db.get('endpoints')}/{self.db_name}",
                "connection.user": db.get("username"),
                "connection.password": db.get("password"),
            }
        )

    @override
    def teardown(self):
        pass

    @property
    @override
    def ready(self):
        return self.helpers.check_data_interfaces_ready([self.DB_CLIENT_REL, self.CONNECT_REL])
