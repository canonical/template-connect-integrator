#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import logging
from unittest.mock import MagicMock, patch

import pytest
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus
from ops.testing import Context, Relation, State
from requests.exceptions import ConnectionError
from src.literals import CONNECT_REL, CONNECTION_ERROR_MSG, REST_PORT

logger = logging.getLogger(__name__)


DATA_REL = "data"


def build_mock_request(
    status_code: int = 200, task_status: str = "UNASSIGNED", connection_error: bool = False
):
    """Builder function for request/response mock objects on Kafka Connect REST API."""

    def _mock_request(*args, **kwargs):
        if connection_error:
            raise ConnectionError()

        api = kwargs.get("api", "")

        response = MagicMock()
        response.status_code = status_code

        if api.endswith("tasks"):
            response.json.return_value = [{"id": {"task": 0}}]
        elif api.endswith("status"):
            response.json.return_value = {"state": task_status}
        else:
            pass

        return response

    return _mock_request


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


def test_charm_connect_client_relation_departed(
    ctx: Context, base_state: State, patched_server, patched_connect_client, plugin_resource
) -> None:
    """Tests relation-departed would stop the connector."""
    # Given
    connect_rel = Relation(CONNECT_REL, CONNECT_REL)
    state_in = dataclasses.replace(
        base_state, resources=[plugin_resource], relations=[connect_rel]
    )

    # When
    state_out = ctx.run(ctx.on.relation_departed(connect_rel), state_in)

    # Then
    assert isinstance(state_out.unit_status, BlockedStatus)
    patched_connect_client.stop_connector.assert_called_once()


@pytest.mark.parametrize("task_status", ["RUNNING", "STOPPED", "PAUSED", "FAILED"])
@pytest.mark.parametrize("status_code", [200, 500])
@pytest.mark.parametrize("with_connection_error", [False, True])
def test_collect_status(
    ctx: Context,
    base_state: State,
    patched_server,
    plugin_resource,
    integrator_has_started,
    task_status,
    status_code,
    with_connection_error,
) -> None:
    """Tests update-status & collect-[unit|app]-status event handler."""
    # Given
    connect_rel = Relation(
        CONNECT_REL,
        CONNECT_REL,
        local_app_data={"username": "username", "password": "password", "endpoints": "endpoints"},
    )
    data_rel = Relation(
        DATA_REL,
        DATA_REL,
        local_app_data={"username": "username", "password": "password", "endpoints": "endpoints"},
    )

    state_in = dataclasses.replace(
        base_state, resources=[plugin_resource], relations=[connect_rel, data_rel]
    )

    # When
    with patch(
        "charms.kafka_connect.v0.integrator.ConnectClient.request",
        build_mock_request(
            task_status=task_status,
            status_code=status_code,
            connection_error=with_connection_error,
        ),
    ):
        state_out = ctx.run(ctx.on.update_status(), state_in)

    # Then
    assert isinstance(state_out.unit_status, ActiveStatus)
    if with_connection_error:
        assert CONNECTION_ERROR_MSG in state_out.unit_status.message
        return

    if status_code == 200:
        assert task_status in state_out.unit_status.message
    else:
        assert "UNKNOWN" in state_out.unit_status.message
