#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for Opensearch sink."""


from charms.data_platform_libs.v0.data_interfaces import (
    OpenSearchRequiresData,
    OpenSearchRequiresEventHandlers,
)
from charms.kafka_connect.v0.integrator import BaseConfigFormatter, BaseIntegrator, ConfigOption
from typing_extensions import override

from workload import PluginServer


class OpensearchConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Aiven Opensearch Sink connector configuration."""

    # configurable options
    topics = ConfigOption(json_key="topics", default="test", description="Topics to read data from.")
    schema_ignore = ConfigOption(json_key="schema.ignore", default=True, description="Whether should include schema in records or not.")
    index_name = ConfigOption(json_key="ignore.index_name", default="test")

    # non-configurable options
    connector_class = ConfigOption(
        json_key="connector.class",
        default="io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
        configurable=False,
    )
    key_ignore = ConfigOption(json_key="key.ignore", default=True, configurable=False)
    tasks_max = ConfigOption(json_key="tasks.max", default=1, configurable=False)
    type_name = ConfigOption(json_key="type.name", default="kafka-connect", configurable=False)


class Integrator(BaseIntegrator):
    """Basic implementation for Kafka Connect Opensearch sink Integrator."""

    name = "opensearch-sink-integrator"
    formatter = OpensearchConfigFormatter
    plugin_server = PluginServer

    CLIENT_REL = "data"
    CONNECT_REL = "connect-client"

    def __init__(self, /, charm, plugin_server_args=[], plugin_server_kwargs={}):
        super().__init__(charm, plugin_server_args, plugin_server_kwargs)

        self.index_name = str(self.charm.config.get("index_name", "test"))

        self.database_requirer_data = OpenSearchRequiresData(
            self.model, self.CLIENT_REL, self.index_name, extra_user_roles="admin"
        )
        self.database = OpenSearchRequiresEventHandlers(self.charm, self.database_requirer_data)

    @override
    def setup(self) -> None:
        db = self.helpers.fetch_all_relation_data(self.CLIENT_REL)
        self.configure(
            {
                "connection.url": f"https://{db.get('endpoints')}",
                "connection.username": db.get("username"),
                "connection.password": db.get("password"),
            }
        )

    @override
    def teardown(self):
        pass

    @property
    @override
    def ready(self):
        return self.helpers.check_data_interfaces_ready([self.CLIENT_REL, self.CONNECT_REL])
