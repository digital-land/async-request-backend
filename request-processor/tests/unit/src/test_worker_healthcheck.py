"""Exercise worker probes without contacting AWS or PostgreSQL."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


PROCESSOR_DIR = Path(__file__).resolve().parents[3]


@pytest.fixture
def run_healthcheck(tmp_path):
    if shutil.which("jq") is None:
        pytest.skip("jq is required by the worker health-check script")
    for command in ("pgrep", "aws", "pg_isready"):
        executable = tmp_path / command
        executable.write_text(
            f"#!{sys.executable}\n"
            "import json, os, pathlib, sys\n"
            "name = pathlib.Path(sys.argv[0]).name\n"
            "with open(os.environ['PROBE_TRACE'], 'a') as stream:\n"
            "    stream.write(json.dumps([name, sys.argv[1:], "
            "os.environ.get('AWS_MAX_ATTEMPTS')]) + '\\n')\n"
            "print('probe output: do-not-log', file=sys.stderr)\n"
            "sys.exit(int(os.environ.get('FAIL_' + name, '0')))\n"
        )
        executable.chmod(0o755)

    def run(**overrides):
        trace = tmp_path / "trace.jsonl"
        env = {
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "DATABASE_URL": "postgresql://user:do-not-log@localhost/test",
            "GIT_COMMIT": 'commit"quoted',
            "PROBE_TRACE": str(trace),
        }
        env.pop("CELERY_TASK_DEFAULT_QUEUE", None)
        env.update(overrides)
        result = subprocess.run(
            ["sh", str(PROCESSOR_DIR / "docker-healthcheck.sh")],
            cwd=PROCESSOR_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        calls = [json.loads(line) for line in trace.read_text().splitlines()]
        return result, calls

    return run


@pytest.mark.parametrize(
    "queue", [None, "", "async-request-high", "async-request-standard"]
)
def test_healthcheck_uses_configured_queue_and_bounded_probes(run_healthcheck, queue):
    env = {} if queue is None else {"CELERY_TASK_DEFAULT_QUEUE": queue}
    result, calls = run_healthcheck(**env)

    assert result.returncode == 0
    assert result.stderr == ""
    output = json.loads(result.stdout)
    assert output["version"] == 'commit"quoted'
    assert all(item["status"] == "HEALTHY" for item in output["dependencies"])
    aws = next(call for call in calls if call[0] == "aws")
    assert aws[1] == [
        "sqs",
        "get-queue-url",
        "--queue-name",
        queue or "celery",
        "--cli-connect-timeout",
        "1",
        "--cli-read-timeout",
        "1",
    ]
    assert aws[2] == "1"
    postgres = next(call for call in calls if call[0] == "pg_isready")
    assert postgres[1][-2:] == ["-t", "1"]


@pytest.mark.parametrize(
    "probe,message,failed_dependency",
    [
        ("pgrep", "Celery process not found", None),
        ("aws", "SQS queue lookup failed or timed out", "sqs"),
        ("pg_isready", "PostgreSQL readiness check failed or timed out", "request-db"),
    ],
)
def test_healthcheck_reports_failure_without_credentials(
    run_healthcheck, probe, message, failed_dependency
):
    result, calls = run_healthcheck(**{f"FAIL_{probe}": "1"})

    assert result.returncode == 1
    assert message in result.stderr
    assert "do-not-log" not in result.stdout + result.stderr
    assert len(calls) == 3
    for dependency in json.loads(result.stdout)["dependencies"]:
        expected = "UNHEALTHY" if dependency["name"] == failed_dependency else "HEALTHY"
        assert dependency["status"] == expected
