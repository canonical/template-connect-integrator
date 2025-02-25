#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from ops.testing import Container, Context, Resource, State
from src.charm import IntegratorCharm
from src.literals import PLUGIN_RESOURCE_KEY, SUBSTRATE

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@pytest.fixture()
def base_state():
    if SUBSTRATE == "k8s":
        state = State(
            leader=True, containers=[Container(name="template-integrator", can_connect=True)]
        )
    else:
        state = State(leader=True)

    return state


@pytest.fixture()
def ctx() -> Context:
    ctx = Context(IntegratorCharm, meta=METADATA, config=CONFIG, actions=ACTIONS, unit_id=0)
    return ctx


@pytest.fixture(scope="module")
def plugin_resource():
    return Resource(name=PLUGIN_RESOURCE_KEY, path="FakePlugin.tar")


@pytest.fixture(autouse=True)
def patched_server():
    with patch("integration.Integrator.plugin_server") as server:
        instance = MagicMock()
        instance.exec.return_value = "done"

        server.return_value = instance
        yield server
