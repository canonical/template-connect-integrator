#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import logging

import pytest
from ops.model import BlockedStatus, MaintenanceStatus
from ops.testing import Context, State
from src.literals import REST_PORT

logger = logging.getLogger(__name__)


def test_charm_with_no_resource(ctx: Context, base_state: State) -> None:
    """Test charm goes into error state without resource being attached."""
    # Given
    state_in = base_state

    # Then unit should be in error
    with pytest.raises(Exception):
        _ = ctx.run(ctx.on.config_changed(), state_in)


def test_charm_with_resource(
    ctx: Context, base_state: State, patched_server, plugin_resource
) -> None:
    """Test charm goes into error state without resource being attached."""
    # Given
    state_in = dataclasses.replace(base_state, resources=[plugin_resource])

    # When
    state_out = ctx.run(ctx.on.config_changed(), state_in)

    # Then
    assert isinstance(state_out.unit_status, BlockedStatus)


@pytest.mark.parametrize("patched_server", [True, False], indirect=True)
def test_charm_start(ctx: Context, base_state: State, patched_server, plugin_resource) -> None:
    """Tests `start` event handler."""
    # Given
    state_in = dataclasses.replace(base_state, resources=[plugin_resource])

    # When
    state_out = ctx.run(ctx.on.start(), state_in)

    # Then
    if patched_server.healthy:
        assert isinstance(state_out.unit_status, BlockedStatus)
    else:
        assert isinstance(state_out.unit_status, MaintenanceStatus)


@pytest.mark.parametrize("patched_server", [True, False], indirect=True)
def test_charm_update_status(
    ctx: Context, base_state: State, patched_server, plugin_resource
) -> None:
    """Tests `update-status` event handler."""
    # Given
    state_in = dataclasses.replace(base_state, resources=[plugin_resource])

    # When
    state_out = ctx.run(ctx.on.update_status(), state_in)

    # Then
    if patched_server.healthy:
        assert isinstance(state_out.unit_status, BlockedStatus)
        assert state_out.opened_ports
        port_obj = next(iter(state_out.opened_ports))
        assert port_obj.port == REST_PORT
    else:
        assert isinstance(state_out.unit_status, MaintenanceStatus)
