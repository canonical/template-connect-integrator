import json
import logging
import random
import re
import socket
import string
from dataclasses import dataclass

import kafka
import kafka.errors
import requests
import yaml
from kafka.admin import NewTopic
from ops.model import Unit
from pytest_operator.plugin import OpsTest

K8S_IMAGE = "ubuntu/python:3.12-24.04_stable"
CONNECT_APP = "kafka-connect"
CONNECT_CHANNEL = "latest/edge"
CONNECT_ADMIN_USER = "admin"
CONNECT_REST_PORT = 8083
KAFKA_APP = "kafka"
KAFKA_APP_B = "kafka-b"
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

JDBC_SOURCE_CONNECTOR_CLASS = "io.aiven.connect.jdbc.JdbcSourceConnector"
JDBC_SINK_CONNECTOR_CLASS = "io.aiven.connect.jdbc.JdbcSinkConnector"
S3_CONNECTOR_CLASS = "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector"


logger = logging.getLogger(__name__)


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


def get_plugin_url(build_dir: str = "./build") -> str:
    """Loads `plugin-url` key from charmcraft.yaml in the build folder."""
    metadata = yaml.safe_load(open(f"{build_dir}/metadata.yaml", "r"))
    links = metadata.get("links", {}).get("plugin-url", [])

    if not links:
        return ""

    return links[0]


async def run_command_on_unit(
    ops_test: OpsTest, unit: Unit, command: str | list[str], container: str | None = None
) -> CommandResult:
    """Runs a command on a given unit and returns the result."""
    command_args = command.split() if isinstance(command, str) else command

    if not container:
        return_code, stdout, stderr = await ops_test.juju("ssh", f"{unit.name}", *command_args)
    else:
        return_code, stdout, stderr = await ops_test.juju(
            "ssh", "--container", container, f"{unit.name}", *command_args
        )

    return CommandResult(return_code=return_code, stdout=stdout, stderr=stderr)


async def get_unit_ipv4_address(ops_test: OpsTest, unit: Unit) -> str | None:
    """A safer alternative for `juju.unit.get_public_address()` which is robust to network changes."""
    _, stdout, _ = await ops_test.juju("ssh", f"{unit.name}", "hostname -i")
    ipv4_matches = re.findall(r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}", stdout)

    if ipv4_matches:
        return ipv4_matches[0]

    return None


def download_file(url: str, dst_path: str) -> None:
    """Downloads a file from given `url` to `dst_path`."""
    response = requests.get(url, stream=True)
    with open(dst_path, mode="wb") as file:
        for chunk in response.iter_content(chunk_size=10 * 1024):
            file.write(chunk)


async def get_kafka_password(ops_test: OpsTest, kafka_application: str = KAFKA_APP) -> str:
    """Gets the credentials for the Kafka application."""
    # can retrieve from any unit running unit so we pick the first
    action = await ops_test.model.applications[kafka_application].units[0].run_action(
        "get-admin-credentials"
    )
    action = await action.wait()
    try:
        password = action.results["password"]
        return password
    except KeyError:
        logger.error("Failed to get password. Action %s. Results %s", action, action.results)
        return None


async def get_secret_data(ops_test: OpsTest, label_pattern: str) -> dict:
    """Gets data content of the secret matching the provided label pattern."""
    _, raw, _ = await ops_test.juju("secrets", "--format", "json")

    secrets_json = json.loads(raw)

    matching_secrets = [
        s for s in secrets_json if re.match(label_pattern, secrets_json[s]["label"])
    ]

    if not matching_secrets:
        raise Exception(f"CNo secrets matching {label_pattern} found!")

    secret_id = matching_secrets[0]

    _, secret_raw, _ = await ops_test.juju(
        "show-secret", "--reveal", "--format", "json", secret_id
    )
    secret_json = json.loads(secret_raw)

    return secret_json[secret_id]["content"]["Data"]


async def assert_messages_produced(
    ops_test: OpsTest,
    kafka_app: str,
    topic: str = "test",
    no_messages: int = 1,
    consumer_group: str | None = None,
) -> None:
    """Asserts `no_messages` has been produced to `topic`."""
    username = "admin"
    password = await get_kafka_password(ops_test, kafka_app)
    server = await get_unit_ipv4_address(ops_test, ops_test.model.applications[kafka_app].units[0])

    messages = []
    try:
        consumer = kafka.KafkaConsumer(
            topic,
            bootstrap_servers=f"{server}:9092",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=username,
            sasl_plain_password=password,
            auto_offset_reset="earliest",
            security_protocol="SASL_PLAINTEXT",
            consumer_timeout_ms=5000,
            group_id=consumer_group,
        )

        for msg in consumer:
            messages.append(msg.value)
    except kafka.errors.UnknownTopicOrPartitionError:
        pass

    consumer.close()

    logger.info("Messages consumed: %s", len(messages))
    assert len(messages) == no_messages


def generate_random_message_with_schema(msg_id: int) -> dict:
    """Generates a random message with predefined schema."""
    random_title = "".join([random.choice(string.ascii_letters) for _ in range(16)])
    random_int = random.randint(10, 1000)
    return {
        "schema": {
            "type": "struct",
            "fields": [
                {"type": "int64", "optional": False, "field": "id"},
                {"type": "string", "optional": False, "field": "title"},
                {"type": "int32", "optional": False, "field": "likes"},
            ],
        },
        "payload": {"id": msg_id, "title": random_title, "likes": random_int},
    }


def patched_dns_lookup(host, port, afi, records: dict = {}):
    """Patched version of `kafka.conn.dns_lookup` which could take extra DNS records, suitable for K8s environments."""
    if host not in records:
        return socket.getaddrinfo(host, port, afi, socket.SOCK_STREAM)

    return socket.getaddrinfo(records[host], port, afi, socket.SOCK_STREAM)


async def produce_messages(
    ops_test: OpsTest, kafka_app: str, topic: str = "test", no_messages: int = 1
) -> None:
    """Creates `topic` and produces `no_messages` to it."""
    # Using internal credentials for kafka
    username = "admin"
    password = await get_kafka_password(ops_test, kafka_app)
    server = await get_unit_ipv4_address(ops_test, ops_test.model.applications[kafka_app].units[0])

    config = {
        "bootstrap_servers": f"{server}:9092",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": username,
        "sasl_plain_password": password,
        "security_protocol": "SASL_PLAINTEXT",
    }

    admin_client = kafka.KafkaAdminClient(**config, client_id="test-admin")

    topic_list = [NewTopic(name=topic, num_partitions=10, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except kafka.errors.TopicAlreadyExistsError:
        pass
    admin_client.close()

    producer = kafka.KafkaProducer(
        **config,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for i in range(1, no_messages + 1):
        producer.send(topic, generate_random_message_with_schema(i))

    producer.close()
