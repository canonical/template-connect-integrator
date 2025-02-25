#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of globals common to the Integrator Charm."""

from typing import Literal

Substrates = Literal["vm", "k8s"]

SUBSTRATE = "vm"

PEER_REL = "peer"
CONNECT_REL = "source"

SERVICE_NAME = "integrator-rest"
SERVICE_PATH = f"/etc/systemd/system/{SERVICE_NAME}.service"
USER = "root"
GROUP = "root"

REST_PORT = 8080
PLUGIN_RESOURCE_KEY = "connect-plugin"
