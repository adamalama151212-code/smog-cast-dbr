"""The local CLI knows every step and its pipeline orders reference existing steps."""

import pytest

from smogcast.cli.main import ORDERS, PACKAGES, main

# Declarative pipelines of the bundle → the local step that computes the same tables.
PIPELINE_STEPS = {"smogcast_stream": ("silver-clean", "pm-stream")}
# Local steps that on Databricks run OUTSIDE the Jobs: the Azure Function (functions/gios_poller) polls the API.
OUTSIDE_DATABRICKS = {("ingest", "gios-registry"), ("ingest", "gios-live")}


def test_orders_reference_existing_steps():
    for order in ORDERS.values():
        for pkg, step in order:
            assert step in PACKAGES[pkg]


def test_steps_command_lists_every_package(capsys):
    assert main(["steps"]) == 0
    out = capsys.readouterr().out
    for pkg in PACKAGES:
        assert f"smogcast-{pkg}" in out


def test_unknown_step_is_rejected():
    with pytest.raises(SystemExit) as exc:
        main(["run", "bronze", "no-such-step"])
    assert exc.value.code == 2


def test_bundle_catalog_matches_the_environment_config():
    from pathlib import Path

    import yaml

    from smogcast.core.config import load_config

    bundle = yaml.safe_load((Path(__file__).resolve().parents[3] / "databricks.yml").read_text(encoding="utf-8"))
    for target in bundle["targets"].values():
        v = target["variables"]
        assert v["catalog"] == load_config(v["env"])["storage"]["catalog"], v


def test_dev_names_the_owner_of_every_job_and_pipeline():
    # Left to the bundle, the owner is whoever deploys, and only a workspace admin may change it: CI (the principal)
    # was refused on Jobs a laptop deploy had created. A new Job or pipeline must get the same entry.
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[3]
    dev = yaml.safe_load((root / "databricks.yml").read_text(encoding="utf-8"))["targets"]["dev"]
    owner = [{"service_principal_name": "${var.jobs_principal}", "level": "IS_OWNER"}]
    assert dev["run_as"] == {"service_principal_name": "${var.jobs_principal}"}
    for kind in ("jobs", "pipelines"):
        declared = set()
        for f in (root / "resources").glob("*.yml"):
            declared |= set(yaml.safe_load(f.read_text(encoding="utf-8"))["resources"].get(kind, {}))
        assert declared and set(dev["resources"][kind]) == declared, kind
        for name, resource in dev["resources"][kind].items():
            assert resource["permissions"] == owner, name
    # one entry per principal: the owner is not listed again among the target's managers
    assert all("service_principal_name" not in p for p in dev["permissions"])


@pytest.mark.parametrize("job_file", ["job_smogcast.yml", "job_smogcast_live.yml", "job_smogcast_fault_demo.yml"])
def test_every_job_task_retries_and_a_failed_run_sends_an_email(job_file):
    from pathlib import Path

    import yaml

    resources = Path(__file__).resolve().parents[3] / "resources"
    job = next(iter(yaml.safe_load((resources / job_file).read_text(encoding="utf-8"))["resources"]["jobs"].values()))
    assert job["email_notifications"]["on_failure"] == ["${var.notification_email}"]
    for task in job["tasks"]:   # a new task must not silently lose the retries
        assert task["max_retries"] == 2 and task["min_retry_interval_millis"] == 600_000, task["task_key"]
        assert task["retry_on_timeout"] is True and task["timeout_seconds"] > 0, task["task_key"]


def test_the_stream_pipeline_e_mails_a_failed_update():
    from pathlib import Path

    import yaml

    res = Path(__file__).resolve().parents[3] / "resources" / "pipeline_smogcast_stream.yml"
    pipeline = yaml.safe_load(res.read_text(encoding="utf-8"))["resources"]["pipelines"]["smogcast_stream"]
    (n,) = pipeline["notifications"]
    assert n["email_recipients"] == ["${var.notification_email}"] and "on-update-failure" in n["alerts"]


def test_the_live_forecast_task_gets_the_job_run_id():
    # retries of the task share the Job run id, so a retry keeps the run kind and does not duplicate the history
    from pathlib import Path

    import yaml

    live = Path(__file__).resolve().parents[3] / "resources" / "job_smogcast_live.yml"
    job = yaml.safe_load(live.read_text(encoding="utf-8"))["resources"]["jobs"]["smogcast_live"]
    params = next(t for t in job["tasks"] if t["task_key"] == "model_forecast")["python_wheel_task"]["parameters"]
    assert params[params.index("--run-id") + 1] == "{{job.run_id}}"


@pytest.mark.parametrize("job_file, order", [("job_smogcast.yml", "run-batch"), ("job_smogcast_live.yml", "run-live"),
                                             ("job_smogcast_fault_demo.yml", "run-fault-demo")])
def test_lakeflow_jobs_run_the_same_steps_as_the_cli(job_file, order):
    from pathlib import Path

    import yaml

    resources = Path(__file__).resolve().parents[3] / "resources"
    job = next(iter(yaml.safe_load((resources / job_file).read_text(encoding="utf-8"))["resources"]["jobs"].values()))
    steps = set()
    for task in job["tasks"]:
        if "pipeline_task" in task:
            # A pipeline task stands for the local step that builds the same tables.
            pipeline = task["pipeline_task"]["pipeline_id"].split(".")[2]   # ${resources.pipelines.<name>.id}
            steps.add(PIPELINE_STEPS[pipeline])
            continue
        if "dashboard_task" in task:   # refreshes the AI/BI dashboard; nothing to refresh locally
            continue
        t = task["python_wheel_task"]
        params = t["parameters"]
        steps.add((t["entry_point"].removeprefix("smogcast-"), params[params.index("--step") + 1]))
        keys = {d["task_key"] for d in task.get("depends_on", [])}
        assert keys <= {x["task_key"] for x in job["tasks"]}, task["task_key"]
    assert steps == set(ORDERS[order]) - OUTSIDE_DATABRICKS
