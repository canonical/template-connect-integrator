#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for Kafka MirrorMaker."""

import logging

from charms.data_platform_libs.v0.data_interfaces import (
    KafkaRequirerData,
    KafkaRequirerEventHandlers,
)
from kafkacl import BaseConfigFormatter, BaseIntegrator, ConfigOption
from typing_extensions import override

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

    @override
    def setup(self) -> None:
        source_data = self.helpers.fetch_all_relation_data(self.SOURCE_REL)
        target_data = self.helpers.fetch_all_relation_data(self.TARGET_REL)
        source_cluster_alias = self.helpers.remote_app_name(self.SOURCE_REL) or "source"
        target_cluster_alias = self.helpers.remote_app_name(self.TARGET_REL) or "target"

        prefix_policy = {}
        if not self.prefix_topics:
            prefix_policy = {
                "replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy"
            }

        common_auth = {
            "source.cluster.bootstrap.servers": source_data.get("endpoints"),
            "target.cluster.bootstrap.servers": target_data.get("endpoints"),
            "source.cluster.security.protocol": "SASL_PLAINTEXT",
            "source.cluster.sasl.mechanism": "SCRAM-SHA-512",
            "source.cluster.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{source_data.get('username')}\" password=\"{source_data.get('password')}\";",
            "target.cluster.security.protocol": "SASL_PLAINTEXT",
            "target.cluster.sasl.mechanism": "SCRAM-SHA-512",
            "target.cluster.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{target_data.get('username')}\" password=\"{target_data.get('password')}\";",
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
                "producer.override.bootstrap.servers": target_data.get("endpoints"),
                "producer.override.security.protocol": "SASL_PLAINTEXT",
                "producer.override.sasl.mechanism": "SCRAM-SHA-512",
                "producer.override.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{target_data.get('username')}\" password=\"{target_data.get('password')}\";",
            }
            | prefix_policy
            | common_auth
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
                "producer.override.bootstrap.servers": target_data.get("endpoints"),
                "producer.override.security.protocol": "SASL_PLAINTEXT",
                "producer.override.sasl.mechanism": "SCRAM-SHA-512",
                "producer.override.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{target_data.get('username')}\" password=\"{target_data.get('password')}\";",
            }
            | common_auth
            | prefix_policy
        )

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
