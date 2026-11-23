"""Static checks on the bundle configuration.

`databricks bundle validate` needs a workspace. These tests catch most
mistakes without one, using the official Python models from the
`databricks-bundles` package, which reject unknown or misspelled fields.
"""

import glob
import os
import re

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESOURCE_FILES = sorted(glob.glob(os.path.join(ROOT, "resources", "*.yml")))


def _load(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _resources():
    out = {}
    for path in RESOURCE_FILES:
        for kind, items in _load(path)["resources"].items():
            for key, spec in items.items():
                out[(kind, key)] = (path, spec)
    return out


def test_databricks_yml_structure():
    config = _load(os.path.join(ROOT, "databricks.yml"))
    assert config["bundle"]["name"] == "pedalhouse"
    assert set(config["targets"]) == {"dev", "prod"}
    assert config["targets"]["dev"]["mode"] == "development"
    assert config["targets"]["prod"]["mode"] == "production"
    assert set(config["variables"]) == {"catalog", "schema"}
    assert config["targets"]["dev"]["variables"]["schema"] != config["targets"]["prod"]["variables"]["schema"]


def test_resources_match_official_models():
    models = pytest.importorskip("databricks.bundles.jobs")
    pipelines = pytest.importorskip("databricks.bundles.pipelines")
    classes = {"jobs": models.Job, "pipelines": pipelines.Pipeline}
    for (kind, key), (path, spec) in _resources().items():
        classes[kind].from_dict(spec)  # raises on unknown or invalid fields


def test_every_referenced_file_exists():
    for (kind, key), (path, spec) in _resources().items():
        text = yaml.safe_dump(spec)
        base = os.path.dirname(path)
        for rel in re.findall(r"notebook_path: (\S+)", text):
            assert os.path.exists(os.path.normpath(os.path.join(base, rel))), rel
        for lib in spec.get("libraries", []):
            include = lib["glob"]["include"].replace("/**", "")
            assert os.path.isdir(os.path.normpath(os.path.join(base, include))), include


def test_resource_references_point_to_real_resources():
    resources = _resources()
    for (kind, key), (path, spec) in resources.items():
        for ref_kind, ref_key in re.findall(r"\$\{resources\.(\w+)\.(\w+)\.", yaml.safe_dump(spec)):
            assert (ref_kind, ref_key) in resources, f"{path}: unknown resource {ref_kind}.{ref_key}"


def test_job_task_graph_is_consistent():
    _, job = _resources()[("jobs", "pedalhouse_daily")]
    keys = {t["task_key"] for t in job["tasks"]}
    for task in job["tasks"]:
        for dep in task.get("depends_on", []):
            assert dep["task_key"] in keys, f"{task['task_key']} depends on unknown {dep['task_key']}"
    # Task values used in the YAML must be produced by a task that exists.
    for task_key in re.findall(r"\{\{tasks\.(\w+)\.values", yaml.safe_dump(job)):
        assert task_key in keys
