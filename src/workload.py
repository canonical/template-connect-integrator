#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Plugin Server Workload."""

import inspect
import logging
import os
import subprocess
from pathlib import Path
from typing import Mapping

import requests
from charms.data_platform_libs.v0.data_interfaces import PLUGIN_URL_NOT_REQUIRED
from charms.kafka_connect.v0.integrator import BasePluginServer
from charms.operator_libs_linux.v1.systemd import (
    daemon_reload,
    service_restart,
    service_running,
    service_stop,
)
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)
from typing_extensions import override

from literals import GROUP, REST_PORT, SERVICE_NAME, SERVICE_PATH, USER

logger = logging.getLogger(__name__)


class NotRequiredPluginServer(BasePluginServer):
    """An implementation for when plugins are not required to be served."""

    def __init__(self, *args, **kwargs):
        self.plugin_url = PLUGIN_URL_NOT_REQUIRED

    @override
    def start(self) -> None:
        logger.info("Plugin server not required - skipping start")

    @override
    def stop(self) -> None:
        logger.info("Plugin server not required - skipping stop")

    @override
    def configure(self) -> None:
        logger.info("Plugin server not required - skipping configure")

    @override
    def health_check(self) -> bool:
        logger.info("Plugin server not required - skipping health check")
        return True


class PluginServer(BasePluginServer):
    """An implementation based on FastAPI & Systemd for BasePluginServer."""

    service: str = SERVICE_NAME

    def __init__(
        self,
        charm_dir: Path = Path("/path/to/charm/"),
        base_address: str = "localhost",
        port: int = REST_PORT,
    ) -> None:
        self.charm_dir = charm_dir
        self.base_address = base_address
        self.port = port

    @override
    def start(self) -> None:
        service_restart(self.service)

    @override
    def stop(self) -> None:
        service_stop(self.service)

    @override
    def configure(self) -> None:
        self.write(content=self.systemd_config + "\n", path=SERVICE_PATH)
        daemon_reload()

    @override
    @retry(
        wait=wait_fixed(1),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(lambda _: True),
        retry_error_callback=lambda _: False,
    )
    def health_check(self) -> bool:
        if not service_running(self.service):
            return False

        try:
            response = requests.get(self.health_url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.info(f"Something went wrong during health check: {e}")
            raise e

    def load_plugin(self, path: str):
        """Loads plugin file for serving."""
        self.exec(f"mv {path} {self.plugin_file_path}")

    def read(self, path: str) -> list[str]:
        """Reads a file from the workload.

        Args:
            path: the full filepath to read from

        Returns:
            List of string lines from the specified path
        """
        if not os.path.exists(path):
            return []
        else:
            with open(path) as f:
                content = f.read().split("\n")

        return content

    def write(self, content: str, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string of content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode) as f:
            f.write(content)

        self.exec(["chown", "-R", f"{USER}:{GROUP}", f"{path}"])

    def exec(
        self,
        command: list[str] | str,
        env: Mapping[str, str] | None = None,
        working_dir: str | None = None,
    ) -> str:
        """Executes a command on the workload substrate.

        Returns None if the command failed to be executed.
        """
        try:
            output = subprocess.check_output(
                command,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                shell=isinstance(command, str),
                env=env,
                cwd=working_dir,
            )
            logger.debug(f"{output=}")
            return output
        except subprocess.CalledProcessError as e:
            logger.error(f"cmd failed - cmd={e.cmd}, stdout={e.stdout}, stderr={e.stderr}")
            raise e

    @property
    def systemd_config(self) -> str:
        """Returns the Systemd configuration for FastAPI service."""
        return inspect.cleandoc(
            f"""
            [Unit]
            Description=Kafka Connect Integrator REST API
            Wants=network.target
            Requires=network.target

            [Service]
            WorkingDirectory={self.charm_dir}/src/rest
            EnvironmentFile=-/etc/environment
            Environment=PORT={self.port}
            Environment=PLUGIN_FILE_PATH={self.plugin_file_path}
            ExecStart={self.charm_dir}/venv/bin/python {self.charm_dir}/src/rest/entrypoint.py
            Restart=always
            Type=simple
        """
        )

    @property
    def plugin_url(self) -> str:
        """Returns the plugin serving URL."""
        return f"http://{self.base_address}:{self.port}/api/v1/plugin"

    @property
    def health_url(self) -> str:
        """Returns the health check URL."""
        return f"http://{self.base_address}:{self.port}/api/v1/healthcheck/liveness"

    @property
    def plugin_file_path(self) -> str:
        """Returns the actual path on workload where plugin is stored."""
        return f"{self.charm_dir}/src/rest/resources/connector-plugin.tar"
