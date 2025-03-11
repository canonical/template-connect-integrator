import json
import random
import re
import string
from dataclasses import dataclass

import kafka
import kafka.errors
import requests
from kafka.admin import NewTopic
from ops.model import Unit
from pytest_operator.plugin import OpsTest

CONNECT_APP = "kafka-connect"
CONNECT_ADMIN_USER = "admin"
CONNECT_REST_PORT = 8083
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
OPENSEARCH_CONNECTOR_LINK = "https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v3.1.1/opensearch-connector-for-apache-kafka-3.1.1.tar"
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


def download_file(url: str, dst_path: str) -> None:
    """Downloads a file from given `url` to `dst_path`."""
    response = requests.get(url, stream=True)
    with open(dst_path, mode="wb") as file:
        for chunk in response.iter_content(chunk_size=10 * 1024):
            file.write(chunk)


async def get_secret_data(ops_test: OpsTest, label_pattern: str) -> dict:
    """Gets data content of the secret matching the provided label pattern."""
    _, raw, _ = await ops_test.juju("secrets", "--format", "json")

    secrets_json = json.loads(raw)

    matching_secrets = [
        s for s in secrets_json if re.match(label_pattern, secrets_json[s]["label"])
    ]

    if not matching_secrets:
        raise Exception(f"No secrets matching {label_pattern} found!")

    secret_id = matching_secrets[0]

    _, secret_raw, _ = await ops_test.juju(
        "show-secret", "--reveal", "--format", "json", secret_id
    )
    secret_json = json.loads(secret_raw)

    return secret_json[secret_id]["content"]["Data"]


async def assert_messages_produced(
    ops_test: OpsTest, kafka_app: str, topic: str = "test", no_messages: int = 1
) -> None:
    """Asserts `no_messages` has been produced to `topic`."""
    data = await get_secret_data(ops_test, r"kafka-client\.[0-9]+\.user\.secret")

    username = data["username"]
    password = data["password"]
    server = await get_unit_ipv4_address(ops_test, ops_test.model.applications[kafka_app].units[0])

    consumer = kafka.KafkaConsumer(
        topic,
        bootstrap_servers=f"{server}:9092",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=username,
        sasl_plain_password=password,
        auto_offset_reset="earliest",
        security_protocol="SASL_PLAINTEXT",
        consumer_timeout_ms=5000,
    )

    messages = []
    for msg in consumer:
        messages.append(msg.value)

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


async def produce_messages(
    ops_test: OpsTest, kafka_app: str, topic: str = "test", no_messages: int = 1
) -> None:
    """Creates `topic` and produces `no_messages` to it."""
    data = await get_secret_data(ops_test, r"kafka-client\.[0-9]+\.user\.secret")

    username = data["username"]
    password = data["password"]
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
