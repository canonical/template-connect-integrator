#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Utility script for generating charmcraft build files."""

import argparse
import inspect
import importlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml
from charms.kafka_connect.v0.integrator import BaseConfigFormatter


@dataclass
class Settings:
    output_path: Path
    module: str = "integration"


def parse_args() -> Settings:
    """Parse CLI args."""
    parser = argparse.ArgumentParser(description="Render config.yaml from `src/integration.py` args.")

    parser.add_argument(
        "output",
        type=Path,
        help="path to the rendered config.yaml",
    )

    parser.add_argument(
        "--impl",
        help="path to the module which implements BaseIntegrator & BaseConfigFormatter, defaults to integration.",
        type=str,
        default="integration"
    )

    args, _ = parser.parse_known_args()

    return Settings(output_path=args.output, module=args.impl)


def render_config(output_path: Path, module: str = "integration") -> str | None:
    """Renders a charmcraft `config.yaml` file based on the implementation of `BaseConfigFormatter` provided in `module`."""
    integration = importlib.import_module(module, package=None)

    formatters = [
        c
        for c in dir(integration)
        if inspect.isclass(getattr(integration, c))
        and c != "BaseConfigFormatter"
        and issubclass(getattr(integration, c), BaseConfigFormatter)
    ]

    if not formatters:
        sys.exit(0)

    if len(formatters) > 1:
        print("Failed: more than one BaseConfigFormatter implementation found, exiting.")
        sys.exit(2)

    formatter = cast(BaseConfigFormatter, getattr(integration, formatters[0]))

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    yaml.safe_dump(formatter.to_config_yaml(), open(output_path / "config.yaml", "w"))

    return integration.__file__


if __name__ == "__main__":
    settings = parse_args()
    integration_module = render_config(output_path=settings.output_path, module=settings.module)
    print(integration_module)
    os.system(f"cp {integration_module} {settings.output_path}/src/integration.py")
