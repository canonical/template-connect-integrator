#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Utility script for generating charmcraft build files."""

import argparse
import importlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import cast

import yaml
from charms.kafka_connect.v0.integrator import BaseConfigFormatter


@dataclass
class Settings:
    """Object for storing settings passed via command line args."""

    output_path: Path
    impl: str = "mysql"


IMPLEMENTATIONS = {
    "mysql": "MySQLConfigFormatter",
    "opensearch": "OpensearchConfigFormatter",
    "postgresql": "PostgresConfigFormatter",
    "s3": "S3ConfigFormatter",
    "mirrormaker": "MirrormakerConfigFormatter"
}


def parse_args() -> Settings:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(
        description="Render config.yaml from `src/integration.py` args."
    )

    parser.add_argument(
        "output",
        type=Path,
        help="path to the rendered config.yaml",
    )

    parser.add_argument(
        "--impl",
        help="path to the module which implements BaseIntegrator & BaseConfigFormatter, defaults to integration.",
        type=str,
        default="mysql",
    )

    args, _ = parser.parse_known_args()

    return Settings(output_path=args.output, impl=args.impl)


def render_config(output_path: Path, module: ModuleType, klass: str) -> str | None:
    """Renders a charmcraft `config.yaml` file based on the implementation of `BaseConfigFormatter` provided in `module`."""
    formatter = cast(BaseConfigFormatter, getattr(module, klass))

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    yaml.safe_dump(formatter.to_config_yaml(), open(output_path / "config.yaml", "w"))


if __name__ == "__main__":
    settings = parse_args()

    if settings.impl not in IMPLEMENTATIONS:
        sys.exit(f"Failed: unrecognized implementation {settings.impl}")

    os.system(f"cp examples/{settings.impl}.py {settings.output_path}/src/integration.py")
    module = importlib.import_module(f"examples.{settings.impl}", package=None)

    render_config(
        output_path=settings.output_path,
        module=module,
        klass=IMPLEMENTATIONS[settings.impl],
    )
