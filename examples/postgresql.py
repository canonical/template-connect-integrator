#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for PostgreSQL source/sink."""


from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequirerData,
    DatabaseRequirerEventHandlers,
)
from charms.kafka_connect.v0.integrator import BaseConfigFormatter, BaseIntegrator, ConfigOption
from typing_extensions import override

from workload import PluginServer


class MySQLConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Aiven JDBC connector configuration."""

    # configurable options [source]
    topic_prefix = ConfigOption(json_key="topic.prefix", default="test_")
    select_mode = ConfigOption(json_key="mode", default="incrementing", mode="source")
    incrementing_column = ConfigOption(
        json_key="incrementing.column.name", default="id", mode="source"
    )
    topic_partitions = ConfigOption(
        json_key="topic.creation.default.partitions", default=10, mode="source"
    )
    topic_replication_factor = ConfigOption(
        json_key="topic.creation.default.replication.factor", default=-1, mode="source"
    )

    # configurable options [sink]
    topics_regex = ConfigOption(json_key="topics.regex", default="test_.*", mode="sink")
    insert_mode = ConfigOption(json_key="insert.mode", default="insert", mode="sink")

    # non-configurable options
    tasks_max = ConfigOption(json_key="tasks.max", default=1, configurable=False)
    auto_create = ConfigOption(json_key="auto.create", default=True, configurable=False)
    auto_evolve = ConfigOption(json_key="auto.evolve", default=True, configurable=False)
    key_converter = ConfigOption(
        json_key="key.converter",
        default="org.apache.kafka.connect.storage.StringConverter",
        configurable=False,
        mode="source",
    )
    value_converter = ConfigOption(
        json_key="value.converter",
        default="org.apache.kafka.connect.json.JsonConverter",
        configurable=False,
    )

    # general charm config
    db_name = ConfigOption(json_key="na", default="test_db", mode="none")


class Integrator(BaseIntegrator):
    """Basic implementation for Kafka Connect MySQL Integrator."""

    name = "mysql-integrator"
    formatter = MySQLConfigFormatter
    plugin_server = PluginServer

    CONNECT_REL = "connect-client"
    DB_CLIENT_REL = "data"

    def __init__(self, /, charm, plugin_server_args=[], plugin_server_kwargs={}):
        super().__init__(charm, plugin_server_args, plugin_server_kwargs)

        self.name = charm.app.name
        self.db_name = str(self.charm.config.get("db_name", "test_db"))

        self.database_requirer_data = DatabaseRequirerData(
            self.model, self.DB_CLIENT_REL, self.db_name, extra_user_roles="admin"
        )
        self.database = DatabaseRequirerEventHandlers(self.charm, self.database_requirer_data)

    @override
    def setup(self) -> None:
        db = self.helpers.fetch_all_relation_data(self.DB_CLIENT_REL)
        connector_class = (
            "io.aiven.connect.jdbc.JdbcSourceConnector"
            if self.mode == "source"
            else "io.aiven.connect.jdbc.JdbcSinkConnector"
        )
        self.configure(
            {
                "connector.class": connector_class,
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
