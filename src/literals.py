#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of globals common to the Integrator Charm."""

from pathlib import Path
from typing import Literal

import yaml

Substrates = Literal["vm", "k8s"]

SUBSTRATE: Substrates = "vm"

try:
    METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
    CHARM_KEY = METADATA["name"]
    SUBSTRATE = "k8s" if "k8s-api" in METADATA.get("assumes", []) else "vm"
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


# NOTE: Connect truststore password is located on the connect charm. This is to avoid
# sharing the password through the config on the integrator charm.
if SUBSTRATE == "k8s":
    CONNECT_TRUSTSTORE_PASSWORD_PATH = "/etc/connect/truststore.password"
    CONNECT_TRUSTSTORE_PATH = "/etc/connect/truststore.jks"
elif SUBSTRATE == "vm":
    CONNECT_TRUSTSTORE_PASSWORD_PATH = (
        "/var/snap/charmed-kafka/current/etc/connect/truststore.password"
    )
    CONNECT_TRUSTSTORE_PATH = "/var/snap/charmed-kafka/current/etc/connect/truststore.jks"
CONNECT_TRUSTSTORE_PASSWORD_KEY = "truststore"
