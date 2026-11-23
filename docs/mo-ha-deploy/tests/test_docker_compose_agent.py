"""Exercise the HA resource agent using isolated Docker, Compose and OCF stubs."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET


AGENT = Path(
    os.environ.get(
        "AGENT_UNDER_TEST",
        Path(__file__).resolve().parents[1] / "agents/heartbeat/docker-compose",
    )
).resolve()

OCF = """\
OCF_SUCCESS=0
OCF_ERR_GENERIC=1
OCF_ERR_ARGS=2
OCF_ERR_UNIMPLEMENTED=3
OCF_NOT_RUNNING=7
ocf_log() { printf '%s\\n' "$*" >&2; }
check_binary() { [ -x "$1" ] || exit 5; }
"""

COMPOSE = """\
#!/bin/sh
printf 'compose %s\\n' "$*" >> "$TRACE"
case "$3" in
    config)
        [ "${CONFIG_ERROR:-0}" = 0 ] || exit "$CONFIG_ERROR"
        case "$4 $5" in
            "--format json") printf '%s\\n' "$CONFIG_JSON" ;;
            "--services ") printf '%s\\n' "$SERVICES" ;;
            *) exit 99 ;;
        esac
        ;;
    ps)
        [ "${COMPOSE_PS_ERROR:-0}" = 0 ] || exit "$COMPOSE_PS_ERROR"
        if [ -f "$COMPOSE_STOPPED" ]; then
            exit 0
        fi
        if [ -f "$COMPOSE_STARTED" ]; then
            ids=$AFTER_UP_IDS
        else
            ids=$COMPOSE_IDS
        fi
        case "$4" in
            -q) printf '%s\\n' "$ids" | sed '/^stopped$/d; /^oneoff$/d; /^init-exited$/d; /^init-failed$/d' ;;
            -aq) printf '%s\\n' "$ids" ;;
            *) exit 99 ;;
        esac
        ;;
    up)
        [ "${UP_ERROR:-0}" = 0 ] || exit "$UP_ERROR"
        touch "$COMPOSE_STARTED"
        ;;
    down)
        [ "${DOWN_ERROR:-0}" = 0 ] || exit "$DOWN_ERROR"
        [ "${DOWN_NO_EFFECT:-0}" = 1 ] || touch "$COMPOSE_STOPPED"
        ;;
    *) exit 99 ;;
esac
"""

DOCKER = """\
#!/bin/sh
printf 'docker %s\\n' "$*" >> "$TRACE"
case "$1" in
    ps)
        [ "${DOCKER_PS_ERROR:-0}" = 0 ] || exit "$DOCKER_PS_ERROR"
        for container in "$CONTAINERS"/*; do
            [ -f "$container" ] || continue
            id=${container##*/}
            if [ "$5" = '{{.ID}} {{.Names}}' ] || [ "$4" = '{{.ID}} {{.Names}}' ]; then
                IFS="$(printf '\\t')" read -r name rest < "$container"
                printf '%s %s\\n' "$id" "$name"
            else
                printf '%s\\t' "$id"
                IFS= read -r labels < "$container"
                state=running
                [ ! -f "$TRACE.$id.stopped" ] || state=exited
                printf '%s\\t%s\\n' "$labels" "$state"
            fi
        done
        ;;
    inspect)
        [ "${INSPECT_ERROR:-0}" = 0 ] || exit "$INSPECT_ERROR"
        if [ "$2" = '--format={{.State.Status}}' ]; then
            shift 2
            for id do
                if [ -f "$TRACE.$id.stopped" ]; then echo exited; else echo running; fi
            done
            exit 0
        fi
        if [ "$2" = '--format={{.State.Running}}' ]; then
            case "$3" in
                stopped|init-exited|init-failed|oneoff) echo false ;;
                invalid) echo unknown ;;
                *) echo true ;;
            esac
            exit
        fi
        shift 2
        for id do
            if [ -f "$TRACE.$id.stopped" ]; then
                echo 'false|exited|0||app|'
                continue
            fi
            case "$id" in
                stopped) echo 'false|exited|0||worker|' ;;
                recoverable)
                    if [ -f "$COMPOSE_STARTED" ]; then echo 'true|running|0||worker|'
                    else echo 'false|exited|0||worker|'; fi ;;
                init-exited) echo 'false|exited|0||init|' ;;
                init-failed) echo 'false|exited|1||init|' ;;
                invalid) echo unknown ;;
                oneoff) echo 'false|exited|0|True|task|' ;;
                task) echo 'true|running|0|True|app|' ;;
                helper) echo 'true|running|0||helper|' ;;
                worker) echo 'true|running|0||worker|' ;;
                app) echo 'true|running|0||app|init:service_completed_successfully:false' ;;
                *) echo 'true|running|0||app|' ;;
            esac
        done
        ;;
    stop)
        [ "${STOP_ERROR:-0}" = 0 ] || exit "$STOP_ERROR"
        shift
        for id do
            [ "${STOP_NO_EFFECT:-0}" = 1 ] || touch "$TRACE.$id.stopped"
        done
        ;;
    kill) exit 0 ;;
    rm)
        [ "${RM_ERROR:-0}" = 0 ] || exit "$RM_ERROR"
        [ "${RM_NO_EFFECT:-0}" = 1 ] || rm -f "$CONTAINERS/$2"
        ;;
    *) exit 99 ;;
esac
"""


class ResourceAgentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.project = self.root / "my-project"
        self.project.mkdir()
        self.yaml = self.project / "docker-compose.yaml"
        self.yaml.write_text("services: {}\n")
        self.containers = self.root / "containers"
        self.containers.mkdir()
        self.trace = self.root / "trace"
        self.trace.touch()
        (self.root / "ocf-shellfuncs").write_text(OCF)
        for name, script in (("compose", COMPOSE), ("docker", DOCKER)):
            stub = self.root / name
            stub.write_text(script)
            stub.chmod(0o700)

    def add_container(self, name, project="", config="", oneoff="False", container_id="owned"):
        (self.containers / container_id).write_text(f"{name}\t{project}\t{config}\t{oneoff}\n")

    def action(self, action, expected, model=None, **settings):
        env = dict(
            PATH=f"{self.root}:/usr/bin:/bin",
            OCF_FUNCTIONS_DIR=str(self.root),
            OCF_RESKEY_binpath=str(self.root / "compose"),
            OCF_RESKEY_dirpath=str(self.project),
            OCF_RESKEY_ymlfile=self.yaml.name,
            TRACE=str(self.trace),
            COMPOSE_STARTED=str(self.root / "started"),
            COMPOSE_STOPPED=str(self.root / "stopped"),
            CONTAINERS=str(self.containers),
            SERVICES="app",
            AFTER_UP_IDS="running",
        )
        env.update(settings)
        if model is None:
            model = {name: {} for name in env["SERVICES"].splitlines()}
        env["CONFIG_JSON"] = json.dumps({"services": model})

        result = subprocess.run(
            [os.environ.get("AGENT_TEST_SHELL", "/bin/sh"), str(AGENT), action],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
        calls = self.trace.read_text().splitlines()
        self.output = result.stdout
        self.assertEqual(result.returncode, expected, f"{result.stderr}\n{calls}")
        return calls

    def assert_no_mutation(self, calls):
        self.assertFalse(
            any(
                call.startswith(("docker kill ", "docker rm ", "docker stop ")) or call.endswith(" up -d")
                for call in calls
            ),
            calls,
        )

    def test_monitor_running(self):
        self.action("monitor", 0, COMPOSE_IDS="running")

    def test_monitor_empty(self):
        self.action("monitor", 7)

    def test_monitor_all_stopped(self):
        self.action("monitor", 7, COMPOSE_IDS="stopped")

    def test_monitor_partially_running(self):
        self.action("monitor", 1, SERVICES="app\nworker", COMPOSE_IDS="running\nstopped")

    def test_monitor_ignores_one_off_containers(self):
        self.action("monitor", 7, COMPOSE_IDS="oneoff")

    def test_monitor_preserves_custom_initialization_service_behavior(self):
        self.action("monitor", 0, SERVICES="app\ninit", COMPOSE_IDS="init-exited\napp")

    def test_missing_service_with_running_helper_or_orphan(self):
        for services in ("app\nhelper", "app"):
            with self.subTest(services=services):
                self.action("monitor", 1, SERVICES=services, COMPOSE_IDS="helper\ntask")
                self.action("start", 1, SERVICES=services, COMPOSE_IDS="helper\ntask")

    def test_monitor_checks_expected_services_in_label_fallback(self):
        self.add_container("my-project-helper-1", "my-project", self.yaml, container_id="helper")
        self.assert_no_mutation(self.action("monitor", 1, SERVICES="app\nhelper"))

    def test_monitor_cannot_read_effective_services(self):
        self.assert_no_mutation(self.action("monitor", 1, COMPOSE_IDS="running", CONFIG_ERROR="9"))

    def test_monitor_failed_initialization_service(self):
        self.action("monitor", 1, SERVICES="app\ninit", COMPOSE_IDS="app\ninit-failed")

    def test_renamed_service_blocks_repair_even_with_matching_ownership(self):
        self.add_container("my-project-worker-1", "my-project", self.yaml, container_id="worker")
        for ids in ("worker", "", "app\nworker"):
            with self.subTest(ids=ids):
                self.assert_no_mutation(self.action("start", 1, COMPOSE_IDS=ids))
                self.assert_no_mutation(self.action("monitor", 1, COMPOSE_IDS=ids))

    def test_exited_retired_service_does_not_affect_health(self):
        self.action("monitor", 0, COMPOSE_IDS="app\nstopped\ninit-failed")

    def test_zero_replica_service_is_optional_until_it_runs(self):
        for disabled in ({"scale": 0}, {"deploy": {"replicas": 0}}):
            model = {"app": {}, "worker": disabled}
            self.action("monitor", 0, model=model, SERVICES="app\nworker", COMPOSE_IDS="app\nstopped")
            self.assert_no_mutation(self.action("start", 1, model=model, COMPOSE_IDS="app\nworker"))
            self.action("start", 0, model=model, SERVICES="app\nworker", AFTER_UP_IDS="app")
            (self.root / "started").unlink()
            self.trace.write_text("")

    def test_owned_oneoff_blocks_start_without_healthy_services(self):
        self.add_container("my-project-app-run", "my-project", self.yaml, "True", "task")
        for ids in ("", "task", "stopped"):
            with self.subTest(ids=ids):
                self.assert_no_mutation(self.action("monitor", 1, COMPOSE_IDS=ids))
                self.assert_no_mutation(self.action("start", 1, COMPOSE_IDS=ids))
        self.action("monitor", 0, COMPOSE_IDS="app\ntask")

    def test_owned_oneoff_blocks_partial_repair_even_when_compose_omits_it(self):
        self.add_container("helper", "my-project", self.yaml, container_id="helper")
        self.add_container("app-run", "my-project", self.yaml, "True", "task")
        self.assert_no_mutation(self.action("start", 1, SERVICES="app\nhelper", COMPOSE_IDS="helper"))

    def test_monitor_counts_distinct_replicas_from_both_sources(self):
        self.add_container("app-1", "my-project", self.yaml, container_id="app")
        model = {"app": {"deploy": {"replicas": 2}}}
        self.action("monitor", 1, model=model, COMPOSE_IDS="app")
        self.action("monitor", 0, model=model, COMPOSE_IDS="app\nrunning")

    def test_monitor_compose_failed(self):
        self.action("monitor", 1, COMPOSE_PS_ERROR="9")

    def test_monitor_inspect_failed(self):
        self.action("monitor", 1, COMPOSE_IDS="running", INSPECT_ERROR="9")

    def test_monitor_inspect_unexpected_output(self):
        self.action("monitor", 1, COMPOSE_IDS="invalid")

    def test_monitor_missing_yaml_with_owned_container_is_read_only(self):
        self.yaml.unlink()
        self.add_container("my-project-helper-1", "override-project", self.yaml, container_id="helper")
        self.assert_no_mutation(self.action("monitor", 1))
        (self.root / "trace.helper.stopped").touch()
        self.action("monitor", 7)

    def test_monitor_missing_yaml_without_containers(self):
        self.yaml.unlink()
        self.action("monitor", 7)

    def test_monitor_detects_previous_project_when_current_project_is_stopped(self):
        self.add_container("matrixone", "previous-project", self.yaml)
        self.assert_no_mutation(self.action("monitor", 0, COMPOSE_IDS="stopped"))

    def test_start_stopped_resource_and_check_state(self):
        calls = self.action("start", 0)
        self.assertIn("compose -f docker-compose.yaml up -d", calls)
        self.assertTrue(calls[-1].startswith("docker inspect "))

    def test_start_already_running(self):
        self.assert_no_mutation(self.action("start", 0, COMPOSE_IDS="running"))

    def test_start_monitor_failed(self):
        self.assert_no_mutation(self.action("start", 1, COMPOSE_PS_ERROR="9"))

    def test_start_when_partially_running(self):
        self.add_container("app-1", "my-project", self.yaml, container_id="running")
        self.add_container("worker-1", "my-project", self.yaml, container_id="recoverable")
        ids = "running\nrecoverable"
        calls = self.action("start", 0, SERVICES="app\nworker", COMPOSE_IDS=ids, AFTER_UP_IDS=ids)
        self.assertEqual(calls.count("compose -f docker-compose.yaml up -d"), 1)

    def test_start_repairs_missing_service_and_verifies_result(self):
        self.add_container("helper-1", "my-project", self.yaml, container_id="helper")
        settings = dict(SERVICES="app\nhelper", COMPOSE_IDS="helper")
        self.action("start", 1, AFTER_UP_IDS="helper", **settings)
        self.assertEqual(self.trace.read_text().count(" up -d"), 1)
        (self.root / "started").unlink()
        self.action("start", 0, AFTER_UP_IDS="app\nhelper", **settings)

    def test_start_rejects_partial_project_when_ownership_is_uncertain(self):
        self.add_container("helper", "my-project", self.yaml, container_id="helper")
        self.add_container("old-app", "old-project", self.yaml, container_id="old")
        self.assert_no_mutation(self.action("start", 1, SERVICES="app\nhelper", COMPOSE_IDS="helper"))

    def test_start_inspect_failure_does_not_attempt_repair(self):
        self.assert_no_mutation(self.action("start", 1, COMPOSE_IDS="running", INSPECT_ERROR="9"))

    def test_start_compose_failed(self):
        self.action("start", 1, UP_ERROR="9")

    def test_start_container_never_started(self):
        self.action("start", 1, AFTER_UP_IDS="")

    def test_start_rejects_partial_startup(self):
        calls = self.action("start", 1, SERVICES="app\nworker", AFTER_UP_IDS="running\nstopped")
        self.assertIn("compose -f docker-compose.yaml up -d", calls)

    def test_stop_compose_success(self):
        calls = self.action("stop", 0, COMPOSE_IDS="running")
        self.assertIn("compose -f docker-compose.yaml down", calls)
        self.assert_no_mutation(calls)

    def test_stop_compose_success_with_absolute_yaml(self):
        calls = self.action("stop", 0, COMPOSE_IDS="running", OCF_RESKEY_ymlfile=str(self.yaml))
        self.assertIn(f"compose -f {self.yaml} down", calls)
        self.assert_no_mutation(calls)

    def test_stop_compose_success_but_container_still_running(self):
        self.action("stop", 1, COMPOSE_IDS="running", DOWN_NO_EFFECT="1")

    def test_stop_compose_success_but_verification_needs_labels(self):
        self.add_container("matrixone", "my-project", self.yaml)
        calls = self.action("stop", 0, COMPOSE_PS_ERROR="9", DOWN_NO_EFFECT="1")
        self.assertIn("docker rm owned", calls)

    def test_stop_detects_previous_project_after_project_name_changes(self):
        self.add_container("matrixone", "previous-project", self.yaml)
        calls = self.action("stop", 0)
        self.assertIn("docker stop owned", calls)
        self.assertNotIn("docker kill owned", calls)

    def test_stop_recovers_after_ineffective_down(self):
        self.add_container("matrixone", "my-project", self.yaml)
        self.action("stop", 1, COMPOSE_IDS="owned", DOWN_NO_EFFECT="1", STOP_NO_EFFECT="1")
        self.action("stop", 0, COMPOSE_IDS="owned", DOWN_NO_EFFECT="1")

    def test_metadata_stop_timeout_matches_deployment(self):
        self.action("meta-data", 0)
        stop = ET.fromstring(self.output).find("./actions/action[@name='stop']")
        self.assertEqual(stop.get("timeout"), "120s")

    def test_stop_compose_failed_fallback(self):
        self.add_container("matrixone", "my-project", self.yaml)
        calls = self.action("stop", 0, DOWN_ERROR="9")
        self.assertIn("docker kill owned", calls)
        self.assertIn("docker rm owned", calls)

    def test_stop_compose_failed_and_fallback_query_failed(self):
        self.assert_no_mutation(self.action("stop", 1, DOWN_ERROR="9", DOCKER_PS_ERROR="9"))

    def test_stop_compose_failed_and_fallback_remove_failed(self):
        self.add_container("matrixone", "my-project", self.yaml)
        self.action("stop", 1, DOWN_ERROR="9", RM_ERROR="9")

    def test_stop_malformed_yaml_uses_labels(self):
        self.add_container("matrixone", "my-project", self.yaml)
        calls = self.action("stop", 0, DOWN_ERROR="9", COMPOSE_PS_ERROR="9")
        self.assertIn("docker rm owned", calls)
        self.assertFalse(any(call.endswith(" ps -aq") for call in calls), calls)
        self.assertFalse((self.containers / "owned").exists())

    def test_stop_missing_compose_binary_uses_labels(self):
        self.add_container("matrixone", "my-project", self.yaml)
        calls = self.action("stop", 0, OCF_RESKEY_binpath=str(self.root / "missing"))
        self.assertIn("docker rm owned", calls)
        self.assertFalse(any(call.startswith("compose ") for call in calls), calls)

    def test_stop_fallback_checks_remaining_containers(self):
        self.add_container("matrixone", "my-project", self.yaml)
        self.action("stop", 1, DOWN_ERROR="9", RM_NO_EFFECT="1", STOP_NO_EFFECT="1")

    def test_stop_fallback_requires_verified_ownership(self):
        self.add_container("matrixone")
        calls = self.action("stop", 1, DOWN_ERROR="9", COMPOSE_IDS="owned")
        self.assert_no_mutation(calls)
        self.assertTrue((self.containers / "owned").exists())

    def test_stop_missing_yaml_with_owned_fixed_name(self):
        self.yaml.unlink()
        self.add_container("matrixone", "override-project", self.yaml)
        calls = self.action("stop", 0)
        self.assertIn("docker kill owned", calls)
        self.assertIn("docker rm owned", calls)

    def test_stop_missing_yaml_with_owned_generated_name(self):
        self.yaml.unlink()
        self.add_container("my-project-matrixone-1", "my-project", self.yaml)
        self.assertIn("docker rm owned", self.action("stop", 0))

    def test_stop_missing_yaml_with_unverified_fixed_name(self):
        self.yaml.unlink()
        self.add_container("matrixone")
        self.assert_no_mutation(self.action("stop", 1))
        self.assertTrue((self.containers / "owned").exists())

    def test_stop_missing_yaml_with_fixed_name_from_other_project(self):
        self.yaml.unlink()
        self.add_container("matrixone", "other", self.root / "other/compose.yaml")
        self.assert_no_mutation(self.action("stop", 1))

    def test_stop_missing_yaml_does_not_touch_other_containers(self):
        self.yaml.unlink()
        self.add_container("matrixone", "override-project", self.yaml)
        self.add_container("other", "other", self.root / "other/compose.yaml", container_id="other")
        self.add_container("task-run-1", "override-project", self.yaml, "True", container_id="task")
        self.assertIn("docker rm owned", self.action("stop", 0))
        self.assertTrue((self.containers / "other").exists())
        self.assertTrue((self.containers / "task").exists())

    def test_stop_missing_yaml_with_multiple_matching_projects(self):
        self.yaml.unlink()
        self.add_container("matrixone", "one", self.yaml)
        self.add_container("two-matrixone-1", "two", self.yaml, container_id="second")
        self.assert_no_mutation(self.action("stop", 1))

    def test_stop_missing_yaml_with_oneoff_from_conflicting_project(self):
        self.yaml.unlink()
        self.add_container("matrixone", "one", self.yaml)
        self.add_container("two-task", "two", self.yaml, "True", "task")
        self.assert_no_mutation(self.action("stop", 1))

    def test_stop_missing_yaml_docker_failed(self):
        self.yaml.unlink()
        self.assert_no_mutation(self.action("stop", 1, DOCKER_PS_ERROR="9"))

    def test_stop_missing_yaml_without_containers(self):
        self.yaml.unlink()
        self.assert_no_mutation(self.action("stop", 0))

    def test_stop_owned_oneoff_with_and_without_yaml(self):
        self.add_container("app-task", "my-project", self.yaml, "True", container_id="task")
        self.add_container("foreign-task", "other", self.root / "other.yaml", "True", "foreign")
        for readable in (True, False):
            with self.subTest(readable=readable):
                if not readable:
                    self.yaml.unlink()
                self.assertIn("docker stop task", self.action("stop", 0))
                self.assertTrue((self.root / "trace.task.stopped").exists())
                self.assertFalse((self.root / "trace.foreign.stopped").exists())
                self.trace.write_text("")
                self.assert_no_mutation(self.action("stop", 0))
                (self.root / "trace.task.stopped").unlink()

    def test_stop_cannot_confirm_oneoff_stopped(self):
        self.add_container("app-task", "my-project", self.yaml, "True", container_id="task")
        self.action("stop", 1, STOP_NO_EFFECT="1")
        self.action("stop", 1, STOP_ERROR="9")

    def test_validate_missing_yaml(self):
        self.yaml.unlink()
        self.action("validate-all", 2)


if __name__ == "__main__":
    unittest.main()
