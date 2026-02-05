#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for MongoDB source/sink."""

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequirerData,
    DatabaseRequirerEventHandlers,
)
from kafkacl import BaseConfigFormatter, BaseIntegrator, ConfigOption
from typing_extensions import override

from workload import PluginServer


class MongoDBConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Debezium MongoDB connector configuration."""

    # configurable options [source]
    topic_prefix = ConfigOption(json_key="topic.prefix", default="test_", mode="source")
    capture_scope = ConfigOption(json_key="capture.scope", default="deployment", mode="source")
    capture_target = ConfigOption(json_key="capture.target", default="", mode="source")
    snapshot_mode = ConfigOption(json_key="snapshot.mode", default="initial", mode="source")

    topic_partitions = ConfigOption(
        json_key="topic.creation.default.partitions", default=10, mode="source"
    )
    topic_replication_factor = ConfigOption(
        json_key="topic.creation.default.replication.factor", default=-1, mode="source"
    )

    # configurable options [sink]
    topics_regex = ConfigOption(json_key="topics.regex", default="test_.*", mode="sink")
    sink_database = ConfigOption(json_key="sink.database", default="", mode="sink")

    # non-configurable options
    tasks_max = ConfigOption(json_key="tasks.max", default=1, configurable=False)
    key_converter = ConfigOption(
        json_key="key.converter",
        default="org.apache.kafka.connect.storage.StringConverter",
        configurable=False,
    )
    value_converter = ConfigOption(
        json_key="value.converter",
        default="org.apache.kafka.connect.json.JsonConverter",
        configurable=False,
    )

    # general charm config
    db_name = ConfigOption(json_key="na", default="test_db", mode="none")


class Integrator(BaseIntegrator):
    """Basic implementation for Kafka Connect MongoDB Integrator."""

    name = "mongodb-integrator"
    formatter = MongoDBConfigFormatter
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
            "io.debezium.connector.mongodb.MongoDbConnector"
            if self.mode == "source"
            else "io.debezium.connector.mongodb.MongoDbSinkConnector"
        )
        self.configure(
            {
                "connector.class": connector_class,
                "mongodb.connection.string": db.get("uris", ""),
                "value.converter.schemas.enable": False,
            }
        )

    @override
    def teardown(self):
        pass

    @property
    @override
    def ready(self):
        return self.helpers.check_data_interfaces_ready([self.DB_CLIENT_REL, self.CONNECT_REL])
