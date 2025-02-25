#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import logging

import pytest
from ops.model import BlockedStatus
from ops.testing import Context, State

logger = logging.getLogger(__name__)


def test_charm_with_no_resource(ctx: Context, base_state: State) -> None:
    """Test charm goes into error state without resource being attached."""
    # Given
    state_in = base_state

    # Then unit should be in error
    with pytest.raises(Exception):
        _ = ctx.run(ctx.on.config_changed(), state_in)


def test_charm_with_resource(ctx: Context, base_state: State, plugin_resource) -> None:
    """Test charm goes into error state without resource being attached."""
    # Given
    state_in = dataclasses.replace(base_state, resources=[plugin_resource])

    # When
    state_out = ctx.run(ctx.on.config_changed(), state_in)

    # Then
    assert isinstance(state_out.unit_status, BlockedStatus)
