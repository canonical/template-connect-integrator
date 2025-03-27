#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Plugin Server Workload."""

import inspect
import logging
import os
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO, Mapping

import requests
from charms.data_platform_libs.v0.data_interfaces import PLUGIN_URL_NOT_REQUIRED
from charms.kafka_connect.v0.integrator import BasePluginServer
from charms.operator_libs_linux.v1.systemd import (
    daemon_reload,
    service_restart,
    service_running,
    service_stop,
)
from ops import Container, pebble
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)
from typing_extensions import override

from literals import CHARM_KEY, REST_PORT, SERVICE_NAME, SERVICE_PATH, SUBSTRATE

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


class BaseWorkload(ABC):
    """Abstract Base Interface for Integrator Workload."""

    charm_dir: Path
    user: str
    group: str

    def __init__(
        self,
        container: Container | None = None,
        charm_dir: Path = Path("/path/to/charm/"),
    ) -> None:
        self.container = container
        self.charm_dir = charm_dir

    @abstractmethod
    def read(self, path: str) -> list[str]:
        """Reads a file from the workload.

        Args:
            path: the full filepath to read from

        Returns:
            List of string lines from the specified path
        """
        ...

    @abstractmethod
    def write(self, content: str | BinaryIO, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string of content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        ...

    @abstractmethod
    def exec(
        self,
        command: list[str] | str,
        env: Mapping[str, str] | None = None,
        working_dir: str | None = None,
    ) -> str:
        """Executes a command on the workload substrate.

        Returns None if the command failed to be executed.
        """
        ...

    @abstractmethod
    def load_plugin(self, path: str) -> None:
        """Loads plugin file from provided `path` to the `plugin_file_path` on workload for serving."""
        ...

    @property
    @abstractmethod
    def ready(self) -> bool:
        """Returns True if the workload service is ready to interact, and False otherwise."""
        ...

    @property
    @abstractmethod
    def plugin_file_path(self) -> str:
        """Returns the actual path on workload where plugin is stored."""
        ...


class VmWorkload(BaseWorkload):
    """An Implementation of Workload Interface for VM."""

    user: str = "root"
    group: str = "root"

    @override
    def read(self, path: str) -> list[str]:
        if not os.path.exists(path):
            return []
        else:
            with open(path) as f:
                content = f.read().split("\n")

        return content

    @override
    def write(self, content: str, path: str, mode: str = "w") -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode) as f:
            f.write(content)

        self.exec(["chown", "-R", f"{self.user}:{self.group}", f"{path}"])

    @override
    def exec(
        self,
        command: list[str] | str,
        env: Mapping[str, str] | None = None,
        working_dir: str | None = None,
    ) -> str:
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

    @override
    def load_plugin(self, path: str):
        self.exec(f"mv {path} {self.plugin_file_path}")

    @property
    @override
    def ready(self) -> bool:
        return True

    @property
    @override
    def plugin_file_path(self) -> str:
        return f"{self.charm_dir}/src/rest/resources/connector-plugin.tar"


class VmPluginServer(BasePluginServer):
    """An implementation based on FastAPI & Systemd for BasePluginServer."""

    service: str = SERVICE_NAME
    service_path: str = SERVICE_PATH

    def __init__(
        self,
        workload: VmWorkload,
        base_address: str = "localhost",
        port: int = REST_PORT,
    ) -> None:
        self.workload = workload
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
        self.workload.write(content=self.systemd_config + "\n", path=self.service_path)
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
            WorkingDirectory={self.workload.charm_dir}/src/rest
            EnvironmentFile=-/etc/environment
            Environment=PORT={self.port}
            Environment=PLUGIN_FILE_PATH={self.workload.plugin_file_path}
            ExecStart={self.workload.charm_dir}/venv/bin/python {self.workload.charm_dir}/src/rest/entrypoint.py
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


# K8s


class K8sWorkload(BaseWorkload):
    """An Implementation of Workload Interface for K8s."""

    user: str = "_daemon_"
    group: str = "_daemon_"

    @override
    def load_plugin(self, path: str):
        self.write(open(path, "rb"), self.plugin_file_path)

    @override
    def read(self, path: str) -> list[str]:
        if not self.container.exists(path):
            return []
        else:
            with self.container.pull(path) as f:
                content = f.read().split("\n")

        return content

    @override
    def write(self, content: str | BinaryIO, path: str, mode: str = "w") -> None:
        self.container.push(path, content, make_dirs=True)

    @override
    def exec(
        self,
        command: list[str] | str,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
    ) -> str:
        command = command if isinstance(command, list) else [command]
        try:
            process = self.container.exec(
                command=command,
                environment=env,
                working_dir=working_dir,
                combine_stderr=True,
            )
            output, _ = process.wait_output()
            return output
        except pebble.ExecError as e:
            logger.debug(e)
            raise e

    @property
    @override
    def ready(self) -> bool:
        return self.container.can_connect()

    @property
    @override
    def plugin_file_path(self) -> str:
        return "/home/connector-plugin.tar"

    @property
    def container(self) -> Container:
        """Returns the K8s Container."""
        if self._container is None:
            raise Exception("Container should be defined for K8s Workloads.")

        return self._container

    @container.setter
    def container(self, value: Container):
        self._container = value


class K8sPluginServer(BasePluginServer):
    """An implementation based on FastAPI & Pebble for BasePluginServer running."""

    service: str = SERVICE_NAME
    stage_dir: str = "/home/workload/"
    python_path: str = f"{stage_dir}/charm/venv/lib/python3.10/site-packages/"

    def __init__(
        self,
        workload: K8sWorkload,
        base_address: str = "localhost",
        port: int = REST_PORT,
    ) -> None:
        self.workload = workload
        self.base_address = base_address
        self.port = port

    @override
    def start(self) -> None:
        self.workload.container.add_layer(CHARM_KEY, self.layer, combine=True)
        self.workload.container.restart(self.service)

    @override
    def stop(self) -> None:
        self.workload.container.stop(self.service)

    @override
    def configure(self) -> None:
        # Make a tar archive from charm container's source
        subprocess.check_output(
            "tar -cvf /home/workload.tar ./charm/", shell=True, cwd=f"{self.workload.charm_dir}/.."
        )

        # Copy the archive to the workload container
        self.workload.write(open("/home/workload.tar", "rb"), "/home/workload.tar")

        try:
            self.workload.container.make_dir(self.stage_dir)
        except pebble.PathError as e:
            if "file exists" not in str(e).lower():
                raise e

        # Untar the archive using python stdlib's tarfile since we don't have tar!
        self.workload.exec(
            ["python3", "-m", "tarfile", "-e", "/home/workload.tar", self.stage_dir]
        )

    @override
    @retry(
        wait=wait_fixed(1),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(lambda _: True),
        retry_error_callback=lambda _: False,
    )
    def health_check(self) -> bool:
        if (
            not self.workload.ready
            or not self.workload.container.get_service(self.service).is_running()
        ):
            return False

        try:
            response = requests.get(self.health_url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.info(f"Something went wrong during health check: {e}")
            raise e

    @property
    def layer(self) -> pebble.Layer:
        """Returns a Pebble configuration layer for FastAPI service."""
        command = f"python3 {self.stage_dir}/charm/src/rest/entrypoint.py"

        layer_config: pebble.LayerDict = {
            "summary": "Plugin Server Layer",
            "description": "Pebble config layer for the Apache Kafka Connect Integrator plugin server",
            "services": {
                self.service: {
                    "override": "merge",
                    "summary": "Plugin Server",
                    "command": command,
                    "startup": "enabled",
                    "user": self.workload.user,
                    "group": self.workload.group,
                    "environment": {
                        "PYTHONPATH": self.python_path,
                        "PORT": str(REST_PORT),
                        "PLUGIN_FILE_PATH": self.workload.plugin_file_path,
                    },
                }
            },
        }
        return pebble.Layer(layer_config)

    @property
    def plugin_url(self) -> str:
        """Returns the plugin serving URL."""
        return f"http://{self.base_address}:{self.port}/api/v1/plugin"

    @property
    def health_url(self) -> str:
        """Returns the health check URL."""
        return f"http://{self.base_address}:{self.port}/api/v1/healthcheck/liveness"


if SUBSTRATE == "k8s":
    Workload: type[BaseWorkload] = K8sWorkload
    PluginServer: type[BasePluginServer] = K8sPluginServer
else:
    Workload: type[BaseWorkload] = VmWorkload
    PluginServer: type[BasePluginServer] = VmPluginServer
