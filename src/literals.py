#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of globals common to the Integrator Charm."""

from pathlib import Path
from typing import Literal

import yaml

Substrates = Literal["vm", "k8s"]

SUBSTRATE = "vm"

try:
    METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
    CHARM_KEY = METADATA["name"]
except FileNotFoundError:
    CHARM_KEY = "template-integrator-charm"

PEER_REL = "peer"
CONNECT_REL = "connect-client"

SERVICE_NAME = "integrator-rest"
SERVICE_PATH = f"/etc/systemd/system/{SERVICE_NAME}.service"
USER = "root"
GROUP = "root"

REST_PORT = 8080
PLUGIN_RESOURCE_KEY = "connect-plugin"

CONNECTION_ERROR_MSG = "error communicating with Kafka Connect, check logs."
