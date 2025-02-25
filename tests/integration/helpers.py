import re
from dataclasses import dataclass

import requests
from ops.model import Unit
from pytest_operator.plugin import OpsTest

CONNECT_APP = "kafka-connect"
KAFKA_APP = "kafka"
KAFKA_CHANNEL = "3/edge"
MYSQL_APP = "mysql"
MYSQL_CHANNEL = "8.0/stable"
POSTGRES_APP = "postgresql"
POSTGRES_CHANNEL = "14/stable"
MYSQL_INTEGRATOR = "mysql-source-integrator"
MYSQL_DB = "test_db"
POSTGRES_INTEGRATOR = "postgres-sink-integrator"
POSTGRES_DB = "sink_db"
PLUGIN_RESOURCE_KEY = "connect-plugin"

JDBC_CONNECTOR_DOWNLOAD_LINK = "https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka/releases/download/v6.10.0/jdbc-connector-for-apache-kafka-6.10.0.tar"
JDBC_SOURCE_CONNECTOR_CLASS = "io.aiven.connect.jdbc.JdbcSourceConnector"
JDBC_SINK_CONNECTOR_CLASS = "io.aiven.connect.jdbc.JdbcSinkConnector"

S3_CONNECTOR_LINK = "https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka/releases/download/v3.1.0/s3-sink-connector-for-apache-kafka-3.1.0.tar"
S3_CONNECTOR_CLASS = "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector"


@dataclass
class CommandResult:
    return_code: int | None
    stdout: str
    stderr: str


@dataclass
class DatabaseFixtureParams:
    """Data model for database test data fixture requests."""

    app_name: str
    db_name: str
    no_tables: int = 1
    no_records: int = 1000


async def run_command_on_unit(
    ops_test: OpsTest, unit: Unit, command: str | list[str]
) -> CommandResult:
    """Runs a command on a given unit and returns the result."""
    command_args = command.split() if isinstance(command, str) else command
    return_code, stdout, stderr = await ops_test.juju("ssh", f"{unit.name}", *command_args)

    return CommandResult(return_code=return_code, stdout=stdout, stderr=stderr)


async def get_unit_ipv4_address(ops_test: OpsTest, unit: Unit) -> str | None:
    """A safer alternative for `juju.unit.get_public_address()` which is robust to network changes."""
    _, stdout, _ = await ops_test.juju("ssh", f"{unit.name}", "hostname -i")
    ipv4_matches = re.findall(r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}", stdout)

    if ipv4_matches:
        return ipv4_matches[0]

    return None


def download_file(url: str, dst_path: str):
    """Downloads a file from given `url` to `dst_path`."""
    response = requests.get(url, stream=True)
    with open(dst_path, mode="wb") as file:
        for chunk in response.iter_content(chunk_size=10 * 1024):
            file.write(chunk)
