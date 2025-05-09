#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of a Kafka Connect Integrator."""


from kafkacl import BaseConfigFormatter, BaseIntegrator
from typing_extensions import override

from literals import CONNECT_REL
from workload import PluginServer


class DummyConfigFormatter(BaseConfigFormatter):
    """Dummy Config Formatter."""

    pass


class Integrator(BaseIntegrator):
    """No-op implementation of BaseIntegrator."""

    name = "dummy-integrator"
    mode = "source"
    formatter = DummyConfigFormatter
    plugin_server = PluginServer

    def __init__(self, /, charm, plugin_server_args=[], plugin_server_kwargs={}):
        super().__init__(charm, plugin_server_args, plugin_server_kwargs)

        self.name = charm.app.name

    @override
    def setup(self) -> None:
        pass

    @override
    def teardown(self):
        pass

    @property
    @override
    def ready(self):
        return self.helpers.check_data_interfaces_ready([CONNECT_REL])
