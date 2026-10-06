"""Verify that the project is installed and exposes its application package."""

import tomllib
from importlib import metadata
from pathlib import Path

import distributed_cloud_scheduler


def test_installed_package_metadata_matches_project() -> None:
    with (Path(__file__).resolve().parents[1] / "pyproject.toml").open("rb") as file:
        project = tomllib.load(file)["project"]

    installed = metadata.distribution(project["name"])
    assert distributed_cloud_scheduler.__name__ == "distributed_cloud_scheduler"
    assert installed.metadata["Name"] == project["name"]
    assert installed.version == project["version"]
    assert installed.metadata["Requires-Python"] == project["requires-python"]
