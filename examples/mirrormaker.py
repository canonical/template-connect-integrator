#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for Kafka MirrorMaker."""

import logging
from functools import cached_property

from charms.data_platform_libs.v0.data_interfaces import (
    KafkaRequirerData,
    KafkaRequirerEventHandlers,
)
from kafkacl import BaseConfigFormatter, BaseIntegrator, ConfigOption
from typing_extensions import override

from literals import SUBSTRATE
from workload import NotRequiredPluginServer

logger = logging.getLogger(__name__)


class MirrormakerConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Kafka MirrorMaker configuration."""

    # Configurable options
    replication_factor = ConfigOption(json_key="replication.factor", default=-1)
    tasks_max = ConfigOption(json_key="tasks.max", default=1, configurable=True)
    key_converter = ConfigOption(
        json_key="key.converter",
        default="org.apache.kafka.connect.converters.ByteArrayConverter",
        configurable=True,
    )
    value_converter = ConfigOption(
        json_key="value.converter",
        default="org.apache.kafka.connect.converters.ByteArrayConverter",
        configurable=True,
    )

    # Non-configurable options
    config_providers = ConfigOption(
        json_key="config.providers",
        default="file",
        configurable=False,
    )
    config_providers_file_class = ConfigOption(
        json_key="config.providers.file.class",
        default="org.apache.kafka.common.config.provider.FileConfigProvider",
        configurable=False,
    )

    # General charm config
    topics = ConfigOption(
        json_key="topics",
        default=".*",
        description="The topics to be replicated.",
        mode="none",
    )
    groups = ConfigOption(
        json_key="groups",
        default=".*",
        description="The groups to be replicated.",
        mode="none",
    )
    prefix_topics = ConfigOption(
        json_key="na",
        default=False,
        description="Whether to prefix the replicated topics with the alias of the source cluster or not.",
        mode="none",
    )


class Integrator(BaseIntegrator):
    """Basic implementation for Kafka MirrorMaker Integrator."""

    name = "kafka-mirrormaker-integrator"
    formatter = MirrormakerConfigFormatter
    plugin_server = NotRequiredPluginServer

    connect_truststore_password_key = "truststore"
    if SUBSTRATE == "k8s":
        connect_truststore_password_path = "/etc/connect/truststore.password"
        connect_truststore_path = "/etc/connect/truststore.jks"
    elif SUBSTRATE == "vm":
        connect_truststore_password_path = (
            "/var/snap/charmed-kafka/current/etc/connect/truststore.password"
        )
        connect_truststore_path = "/var/snap/charmed-kafka/current/etc/connect/truststore.jks"

    SOURCE_REL = "source"
    TARGET_REL = "target"
    CONNECT_REL = "connect-client"

    def __init__(self, /, charm, plugin_server_args=[], plugin_server_kwargs={}):
        super().__init__(charm, plugin_server_args, plugin_server_kwargs)
        self.name = charm.app.name
        self.prefix_topics = bool(self.charm.config.get("prefix_topics", False))
        self.topics = self.charm.config.get("topics", ".*")
        self.groups = self.charm.config.get("groups", ".*")
        # Not used, but required by the Kafka relation. Will create an ACL on Kafka
        self.topic_name = "__mirrormaker-user"

        self.source_requirer_data = KafkaRequirerData(
            model=self.model,
            relation_name=self.SOURCE_REL,
            topic=self.topic_name,
            extra_user_roles="admin",
        )
        self.source = KafkaRequirerEventHandlers(self.charm, self.source_requirer_data)

        self.target_requirer_data = KafkaRequirerData(
            model=self.model,
            relation_name=self.TARGET_REL,
            topic=self.topic_name,
            extra_user_roles="admin",
        )
        self.target = KafkaRequirerEventHandlers(self.charm, self.target_requirer_data)

    @cached_property
    def source_data(self):
        """Return the source cluster data."""
        return self.helpers.fetch_all_relation_data(self.SOURCE_REL)

    @cached_property
    def target_data(self):
        """Return the target cluster data."""
        return self.helpers.fetch_all_relation_data(self.TARGET_REL)

    @cached_property
    def common_auth(self):
        """Return the common authentication configuration for both source and target clusters."""
        common_auth = {
            "source.cluster.bootstrap.servers": self.source_data.get("endpoints"),
            "target.cluster.bootstrap.servers": self.target_data.get("endpoints"),
            "source.cluster.security.protocol": "SASL_PLAINTEXT",
            "source.cluster.sasl.mechanism": "SCRAM-SHA-512",
            "source.cluster.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{self.source_data.get('username')}\" password=\"{self.source_data.get('password')}\";",
            "target.cluster.security.protocol": "SASL_PLAINTEXT",
            "target.cluster.sasl.mechanism": "SCRAM-SHA-512",
            "target.cluster.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{self.target_data.get('username')}\" password=\"{self.target_data.get('password')}\";",
        }
        if self.source_tls_enabled:
            tls_config = {
                "source.cluster.security.protocol": "SASL_SSL",
                "source.cluster.ssl.truststore.location": self.connect_truststore_path,
                "source.cluster.ssl.truststore.password": f"${{file:{self.connect_truststore_password_path}:{self.connect_truststore_password_key}}}",
            }
            common_auth.update(tls_config)
        if self.target_tls_enabled:
            tls_config = {
                "target.cluster.security.protocol": "SASL_SSL",
                "target.cluster.ssl.truststore.location": self.connect_truststore_path,
                "target.cluster.ssl.truststore.password": f"${{file:{self.connect_truststore_password_path}:{self.connect_truststore_password_key}}}",
            }
            common_auth.update(tls_config)

        return common_auth

    @cached_property
    def producer_override(self):
        """Return the producer override configuration for target clusters."""
        producer_override = {
            "producer.override.bootstrap.servers": self.target_data.get("endpoints"),
            "producer.override.security.protocol": "SASL_PLAINTEXT",
            "producer.override.sasl.mechanism": "SCRAM-SHA-512",
            "producer.override.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{self.target_data.get('username')}\" password=\"{self.target_data.get('password')}\";",
        }
        if self.target_tls_enabled:
            producer_override.update(
                {
                    "producer.override.security.protocol": "SASL_SSL",
                    "producer.override.ssl.truststore.location": self.connect_truststore_path,
                    "producer.override.ssl.truststore.password": f"${{file:{self.connect_truststore_password_path}:{self.connect_truststore_password_key}}}",
                }
            )
        return producer_override

    @cached_property
    def source_tls_enabled(self) -> bool:
        """Return whether TLS is enabled for the source cluster."""
        return bool(self.source_data.get("tls", False))

    @cached_property
    def target_tls_enabled(self) -> bool:
        """Return whether TLS is enabled for the target cluster."""
        return bool(self.target_data.get("tls", False))

    @override
    def setup(self) -> None:
        source_cluster_alias = self.helpers.remote_app_name(self.SOURCE_REL) or "source"
        target_cluster_alias = self.helpers.remote_app_name(self.TARGET_REL) or "target"

        prefix_policy = {}
        if not self.prefix_topics:
            prefix_policy = {
                "replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy"
            }

        mirror_source = (
            {
                "name": "source",
                "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
                "clusters": f"{source_cluster_alias},{target_cluster_alias}",
                "source.cluster.alias": source_cluster_alias,
                "target.cluster.alias": target_cluster_alias,
                "topics": self.topics,
                "groups": self.groups,
                "replication.policy.separator": ".replica.",
                "topics.exclude": f".*[-.]internal,.*replica.*,__.*,connect-.*,{target_cluster_alias}.*",
                "groups.exclude": "console-consumer-.*, connect-.*, __.*",
                "offset-syncs.topic.location": "target",
                "offset-syncs.topic.replication.factor": -1,
                "sync.topic.acls.enabled": True,
                "sync.topic.configs.enabled": True,
                "sync.topic.configs.interval.seconds": 5,
                "refresh.topics.enabled": True,
                "refresh.topics.interval.seconds": 5,
                "refresh.groups.enabled": True,
                "refresh.groups.interval.seconds": 5,
                "consumer.auto.offset.reset": "earliest",
                "producer.enable.idempotence": "true",
            }
            | prefix_policy
            | self.common_auth
            | self.producer_override
        )

        mirror_checkpoint = (
            {
                "name": "checkpoint",
                "connector.class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
                "clusters": f"{source_cluster_alias},{target_cluster_alias}",
                "source.cluster.alias": source_cluster_alias,
                "target.cluster.alias": target_cluster_alias,
                "groups": self.groups,
                "groups.exclude": "console-consumer-.*, connect-.*, __.*",
                "replication.policy.separator": ".replica.",
                "consumer.auto.offset.reset": "earliest",
                "offset-syncs.topic.location": "target",
                "checkpoints.topic.replication.factor": -1,
                "emit.checkpoints.enabled": True,
                "emit.checkpoints.interval.seconds": 5,
                "refresh.groups.enabled": True,
                "refresh.groups.interval.seconds": 5,
                "sync.group.offsets.enabled": True,
                "sync.group.offsets.interval.seconds": 5,
            }
            | prefix_policy
            | self.common_auth
            | self.producer_override
        )

        # TODO: To be implemented in a follow-up PR
        # Commented out for now, as it is not needed for the current implementation
        # mirror_heartbeat = (
        #     {
        #         "name": "heartbeat",
        #         "connector.class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
        #         "emit.heartbeats.enabled": True,
        #         "consumer.auto.offset.reset": "earliest",
        #     }
        #     | common_auth
        # )

        self.configure([mirror_source, mirror_checkpoint])

    @override
    def teardown(self):
        if not self._peer_relation:
            return

        logger.info("Removing configuration from peer relation")
        self._peer_unit_interface.update_relation_data(
            self._peer_relation.id, data={self.CONFIG_SECRET_FIELD: ""}
        )

    @property
    @override
    def ready(self):
        return self.helpers.check_data_interfaces_ready(
            [self.SOURCE_REL, self.TARGET_REL, self.CONNECT_REL]
        )
