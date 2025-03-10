#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Apache Kafka Connect Integrator Charmed Operator."""

import logging

from ops.charm import CharmBase, CollectStatusEvent, StartEvent, UpdateStatusEvent
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, ModelError

from integration import Integrator
from literals import (
    CHARM_KEY,
    PLUGIN_RESOURCE_KEY,
    REST_PORT,
    SUBSTRATE,
)
from models import Context

logger = logging.getLogger(__name__)


class IntegratorCharm(CharmBase):
    """Generic Integrator Charm."""

    def __init__(self, *args):
        super().__init__(*args)

        self.name = CHARM_KEY
        self.context = Context(self, substrate=SUBSTRATE)

        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.update_status, self._update_status)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.collect_unit_status, self._on_collect_status)
        self.framework.observe(self.on.collect_app_status, self._on_collect_status)

        self.integrator = Integrator(
            self,
            plugin_server_kwargs={
                "charm_dir": self.charm_dir,
                "base_address": self.context.unit.internal_address,
                "port": REST_PORT,
            },
        )

    def _on_start(self, _: StartEvent) -> None:
        """Handler for `start` event."""
        if self.integrator.server.health_check():
            return

        self.integrator.server.configure()
        self.integrator.server.start()
        logger.info(f"Plugin server started @ {self.integrator.plugin_url}")

    def _update_status(self, event: UpdateStatusEvent) -> None:
        """Handler for `update-status` event."""
        if not self.integrator.server.health_check():
            self.on.start.emit()

        self.unit.set_ports(REST_PORT)

    def _on_config_changed(self, _) -> None:
        """Handler for `config-changed` event."""
        resource_path = None
        try:
            resource_path = self.model.resources.fetch(PLUGIN_RESOURCE_KEY)
            self.integrator.server.load_plugin(  # pyright: ignore[reportAttributeAccessIssue]
                resource_path
            )
        except RuntimeError as e:
            logger.error(f"Resource {PLUGIN_RESOURCE_KEY} not defined in the charm build.")
            raise e
        except (NameError, ModelError) as e:
            logger.error(f"Resource {PLUGIN_RESOURCE_KEY} not found or could not be downloaded.")
            raise e

        self.integrator.server.configure()

    def _on_collect_status(self, event: CollectStatusEvent):
        """Handler for `collect-status` event."""
        if not self.integrator.server.health_check():
            event.add_status(MaintenanceStatus("Setting up the integrator..."))
            return

        if not self.integrator.ready:
            event.add_status(
                BlockedStatus(
                    "Integrator not ready to start, check if all relations are setup successfully."
                )
            )
            return

        try:
            event.add_status(ActiveStatus(f"Task Status: {self.integrator.task_status}"))
        except Exception as e:
            logger.error(e)
            event.add_status(
                ActiveStatus("Task Status: error communicating with Kafka Connect, check logs.")
            )


if __name__ == "__main__":
    main(IntegratorCharm)
