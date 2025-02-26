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
from charms.kafka_connect.v0.integrator import BaseConfigFormatter, ConfigOption

TYPE_MAPPER = {str: "string", int: "int", bool: "boolean"}

DEFAULT_OPTIONS = {
    "mode": {
        "description": 'Integrator mode, either "source" or "sink"',
        "type": "string",
        "default": "source",
    }
}


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

    args, var_args = parser.parse_known_args()

    # if not bundle_args.template.is_file():
    #     raise FileNotFoundError(f"No such file: {bundle_args.template}")

    # bundle_variables = read_bundle_template(bundle_args.template)[1]

    template_variables = ["year", "month"]

    variable_parser = argparse.ArgumentParser()
    for var in template_variables:
        variable_parser.add_argument("--" + var, type=str)

    # # Parse variable args and keep only the once that are not None, otherwise they will be passed
    # # to the template and be considered as "defined", resulting in:
    # # jinja2.exceptions.UndefinedError: 'None' has no attribute 'endswith'
    variables = {
        k: v for k, v in vars(variable_parser.parse_args(var_args)).items() if v is not None
    }

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

    config = {"options": dict(DEFAULT_OPTIONS)}
    for _attr in dir(formatter):

        if _attr.startswith("__"):
            continue

        attr = getattr(formatter, _attr)

        if isinstance(attr, ConfigOption):
            option = cast(ConfigOption, attr)

            if not option.configurable:
                continue

            config["options"][_attr] = {
                "default": option.default,
                "type": TYPE_MAPPER.get(type(option.default), "string"),
            }

            if option.description:
                config["options"][_attr]["description"] = option.description
    
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    yaml.safe_dump(config, open(output_path / "config.yaml", "w"))

    return integration.__file__


if __name__ == "__main__":
    settings = parse_args()
    integration_module = render_config(output_path=settings.output_path, module=settings.module)
    print(integration_module)
    os.system(f"cp {integration_module} {settings.output_path}/src/integration.py")
