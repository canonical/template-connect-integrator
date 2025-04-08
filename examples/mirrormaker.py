#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for Kafka MirrorMaker."""


from charms.data_platform_libs.v0.data_interfaces import (
    KafkaRequirerData,
    KafkaRequirerEventHandlers,
)
from charms.kafka_connect.v0.integrator import BaseConfigFormatter, BaseIntegrator, ConfigOption
from typing_extensions import override

from workload import NotRequiredPluginServer


class MirrormakerConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Kafka MirrorMaker configuration."""

    # configurable options
    topics = ConfigOption(json_key="topics", default=".*")
    topic_replication_factor = ConfigOption(json_key="replication.factor", default=-1)
    topics_exclude = ConfigOption(
        json_key="topics.exclude",
        default=".*[-.]internal,.*.replica,__.*,.*-config,.*-status,.*-offset",
    )
    tasks_max = ConfigOption(json_key="tasks.max", default=1, configurable=True)

    # non-configurable options
    clusters = ConfigOption(json_key="clusters", default="source,target", configurable=False)
    source_cluster_alias = ConfigOption(
        json_key="source.cluster.alias", default="source", configurable=False
    )
    target_cluster_alias = ConfigOption(
        json_key="target.cluster.alias", default="target", configurable=False
    )
    auto_create = ConfigOption(json_key="auto.create", default=True, configurable=False)
    emit_heartbeats = ConfigOption(
        json_key="emit.heartbeats.enabled", default=True, configurable=False
    )
    emit_checkpoints = ConfigOption(
        json_key="emit.checkpoints.enabled", default=True, configurable=False
    )
    emit_heartbeats = ConfigOption(
        json_key="emit.heartbeats.enabled", default=True, configurable=False
    )

    # General charm config
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
                "sync.topic.acls.enabled": True,
                "sync.topic.configs.enabled": True,
                "sync.topic.configs.interval.seconds": 5,
                "refresh.topics.enabled": True,
                "refresh.topics.interval.seconds": 5,
                "refresh.groups.enabled": True,
                "refresh.groups.interval.seconds": 10,
                "groups.exclude": "console-consumer-.*,connect-.*,__.*",
                "consumer.auto.offset.reset": "earliest",
                "producer.max.block.ms": 10000,
                "producer.linger.ms": 500,
                "producer.retry.backoff.ms": 1000,
            }
            | prefix_policy
            | common_auth
        )

        mirror_heartbeat = (
            {
                "name": "heartbeat",
                "connector.class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
                "emit.heartbeats.enabled": True,
                "consumer.auto.offset.reset": "earliest",
            }
            | prefix_policy
            | common_auth
        )

        # TODO: to add?
        # "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        # "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        mirror_checkpoint = (
            {
                "name": "checkpoint",
                "connector.class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
                "groups.exclude": "console-consumer-.*, connect-.*, __.*",
                "consumer.auto.offset.reset": "earliest",
                "refresh.groups.enabled": True,
                "refresh.groups.interval.seconds": 5,
                "sync.group.offsets.interval.seconds": 5,
                "producer.linger.ms": 500,
                "producer.retry.backoff.ms": 1000,
                "producer.max.block.ms": 10000,
            }
            | prefix_policy
            | common_auth
        )

        self.configure([mirror_source, mirror_checkpoint, mirror_heartbeat])

    @override
    def teardown(self):
        pass

    @property
    @override
    def ready(self):
        return self.helpers.check_data_interfaces_ready(
            [self.SOURCE_REL, self.TARGET_REL, self.CONNECT_REL]
        )
