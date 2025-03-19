#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for Kafka MirrorMaker."""


from charms.kafka_connect.v0.integrator import BaseConfigFormatter, BaseIntegrator
from typing_extensions import override

from workload import NotRequiredPluginServer


class MirrormakerConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Kafka MirrorMaker configuration."""


class Integrator(BaseIntegrator):
    """Basic implementation for Kafka MirrorMaker Integrator."""

    name = "kafka-mirrormaker-integrator"
    formatter = MirrormakerConfigFormatter
    plugin_server = NotRequiredPluginServer

    CLIENT_REL = "data"
    CONNECT_REL = "connect-client"

    def __init__(self, /, charm, plugin_server_args=[], plugin_server_kwargs={}):
        super().__init__(charm, plugin_server_args, plugin_server_kwargs)

    @override
    def setup(self) -> None:
        pass

    @override
    def teardown(self):
        pass

    @property
    @override
    def ready(self):
        pass
