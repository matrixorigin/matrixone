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
        if [ "${CONFIG_FAIL_ONCE:-0}" = 1 ] && [ ! -f "$CONFIG_ONCE_MARK" ]; then
            : > "$CONFIG_ONCE_MARK"
            echo "Error: compose config failed" >&2
            exit 1
        fi
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
        if [ -n "${VANISH_ID:-}" ] && [ -f "$VANISHED" ]; then
            if [ "${PS_ERROR_AFTER_VANISH:-0}" != 0 ]; then
                exit "$PS_ERROR_AFTER_VANISH"
            fi
            ids=$(printf '%s\n' "$ids" | grep -F -vx "$VANISH_ID" || true)
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
        # COMPOSE_REMOVE_ORPHANS supplies the default. An explicit flag wins,
        # including --remove-orphans=false.
        remove_orphans=0
        case "${COMPOSE_REMOVE_ORPHANS:-}" in
            true|TRUE|1|yes|YES) remove_orphans=1 ;;
        esac
        for arg in "$@"; do
            case "$arg" in
                --remove-orphans|--remove-orphans=true|--remove-orphans=1) remove_orphans=1 ;;
                --remove-orphans=false|--remove-orphans=0) remove_orphans=0 ;;
            esac
        done
        if [ -n "$STOP_BARRIER" ]; then
            i=0
            while [ ! -f "$STOP_BARRIER.started" ] && [ "$i" -lt 40 ]; do
                i=$((i + 1))
                sleep 0.02
            done
            if [ -f "$STOP_BARRIER.started" ] && [ ! -f "$STOP_BARRIER.finished" ]; then
                printf 'down overlapped stop\\n' >> "$TRACE"
            fi
            touch "$STOP_BARRIER.seen"
        fi
        if [ -n "${DOWN_ELAPSED:-}" ] && [ -n "${DATE_OFFSET_FILE:-}" ]; then
            printf '%s\\n' "$DOWN_ELAPSED" > "$DATE_OFFSET_FILE"
        fi
        if [ "${DOWN_NO_EFFECT:-0}" != 1 ]; then
            touch "$COMPOSE_STOPPED"
            # down stops only the current project's containers, in Compose's
            # dependency order. Record that result so a later Docker stop is
            # not required for those ids.
            for id in $COMPOSE_IDS; do
                [ -n "$id" ] || continue
                printf 'down ordered %s\\n' "$id" >> "$TRACE"
                touch "$TRACE.$id.stopped"
            done
            if [ "$remove_orphans" = 1 ]; then
                orphans=$(python3 -c '
import json, os, re
from pathlib import Path
document = json.loads(os.environ.get("CONFIG_JSON") or "{}")
services = set(document.get("services") or {})
raw = document.get("name") or ""
if not isinstance(raw, str):
    raw = ""
project = re.sub(r"[^a-z0-9_-]", "", raw.lower())
if not project:
    project = re.sub(r"[^a-z0-9_-]", "", os.path.basename(os.getcwd()).lower())
root = Path(os.environ["CONTAINERS"])
for path in sorted(root.iterdir()):
    if not path.is_file() or path.name.endswith(".after"):
        continue
    parts = path.read_text().split("\\t")
    parts[-1] = parts[-1].rstrip("\\n")
    if len(parts) < 5:
        continue
    _, container_project, _, oneoff, service = parts[:5]
    if oneoff.lower() == "true" or container_project != project:
        continue
    if service and service not in services:
        print(path.name)
')
                for id in $orphans; do
                    if [ -f "$TRACE.$id.stop-claimed" ]; then
                        printf 'down stop repeated %s\\n' "$id" >> "$TRACE"
                    else
                        touch "$TRACE.$id.stop-claimed"
                        printf 'down stop %s\\n' "$id" >> "$TRACE"
                    fi
                    touch "$TRACE.$id.stopped"
                done
            fi
        fi
        ;;
    *) exit 99 ;;
esac
"""

DOCKER = """\
#!/bin/sh
printf 'docker %s\\n' "$*" >> "$TRACE"
case "$1" in
    ps)
        if [ "${DOCKER_PS_FAIL_COUNT:-0}" -gt 0 ]; then
            ps_count=0
            [ -f "$DOCKER_PS_COUNT_FILE" ] && ps_count=$(cat "$DOCKER_PS_COUNT_FILE")
            ps_count=$((ps_count + 1))
            printf '%s\\n' "$ps_count" > "$DOCKER_PS_COUNT_FILE"
            if [ "$ps_count" -le "${DOCKER_PS_FAIL_COUNT}" ]; then
                echo "Error: docker ps failed" >&2
                exit 1
            fi
        fi
        if [ "${DOCKER_PS_FAIL_ONCE:-0}" = 1 ] && [ ! -f "$DOCKER_PS_ONCE_MARK" ]; then
            : > "$DOCKER_PS_ONCE_MARK"
            echo "Error: docker ps failed" >&2
            exit 1
        fi
        [ "${DOCKER_PS_ERROR:-0}" = 0 ] || exit "$DOCKER_PS_ERROR"
        for container in "$CONTAINERS"/*; do
            [ -f "$container" ] || continue
            id=${container##*/}
            case "$id" in
                *.after) continue ;;
            esac
            if [ -n "${VANISH_ID:-}" ] && [ -f "$VANISHED" ] && [ "$id" = "$VANISH_ID" ]; then
                continue
            fi
            if [ -f "$container.after" ] && [ ! -f "$COMPOSE_STARTED" ]; then
                continue
            fi
            if [ "$5" = '{{.ID}} {{.Names}}' ] || [ "$4" = '{{.ID}} {{.Names}}' ]; then
                IFS="$(printf '\\t')" read -r name rest < "$container"
                printf '%s %s\\n' "$id" "$name"
            else
                printf '%s\\t' "$id"
                IFS= read -r labels < "$container"
                state=running
                case "$id" in
                    former-init) state=exited ;;
                    *) [ ! -f "$TRACE.$id.stopped" ] || state=exited ;;
                esac
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
        if [ "${INSPECT_FAIL_ONCE:-0}" = 1 ] && [ ! -f "$INSPECT_ONCE_MARK" ]; then
            : > "$INSPECT_ONCE_MARK"
            echo "Error: inspection failed" >&2
            exit 1
        fi
        if [ -n "${VANISH_ID:-}" ] && [ ! -f "$VANISHED" ]; then
            for id do
                if [ "$id" = "$VANISH_ID" ]; then
                    printf '%s\n' "$id" > "$VANISHED"
                    echo "Error: No such object: $id" >&2
                    exit 1
                fi
            done
        fi
        if [ -f "${VANISHED:-/nonexistent}" ] && [ "${INSPECT_ERROR_AFTER_VANISH:-0}" != 0 ]; then
            exit "$INSPECT_ERROR_AFTER_VANISH"
        fi
        for id do
            if [ -n "${VANISH_ID:-}" ] && [ -f "$VANISHED" ] && [ "$id" = "$VANISH_ID" ]; then
                echo "Error: No such object: $id" >&2
                exit 1
            fi
            if [ -f "$TRACE.$id.stopped" ]; then
                echo 'false|exited|0||app|'
                continue
            fi
            case "$id" in
                stopped) echo 'false|exited|0||worker|' ;;
                recoverable)
                    if [ -f "$COMPOSE_STARTED" ]; then echo 'true|running|0||worker|'
                    else echo 'false|exited|0||worker|'; fi ;;
                init-exited*) echo 'false|exited|0||init|' ;;
                init-failed) echo 'false|exited|1||init|' ;;
                init-running*) echo 'true|running|0||init|' ;;
                former-init) echo 'false|exited|0||init|' ;;
                retired) echo 'false|exited|0||app|' ;;
                retired-failed) echo 'false|exited|1||app|' ;;
                invalid) echo unknown ;;
                oneoff) echo 'false|exited|0|True|task|' ;;
                task) echo 'true|running|0|True|app|' ;;
                helper) echo 'true|running|0||helper|' ;;
                worker) echo 'true|running|0||worker|' ;;
                app) echo 'true|running|0||app|init:service_completed_successfully:false' ;;
                app-plain) echo 'true|running|0||app|' ;;
                *) echo 'true|running|0||app|' ;;
            esac
        done
        ;;
    stop)
        if [ "${STOP_FAIL_ONCE:-0}" = 1 ] && [ ! -f "$STOP_ONCE_MARK" ]; then
            : > "$STOP_ONCE_MARK"
            echo "Error: docker stop failed" >&2
            exit 1
        fi
        [ "${STOP_ERROR:-0}" = 0 ] || exit "$STOP_ERROR"
        shift
        ids=
        while [ $# -gt 0 ]; do
            case "$1" in
                -t|--time)
                    shift
                    [ $# -gt 0 ] || break
                    shift
                    ;;
                --time=*|-t*)
                    shift
                    ;;
                *)
                    ids="$ids $1"
                    shift
                    ;;
            esac
        done
        set -- $ids
        for id do
            if [ -f "$TRACE.$id.stop-claimed" ]; then
                printf 'docker stop repeated %s\\n' "$id" >> "$TRACE"
            else
                touch "$TRACE.$id.stop-claimed"
            fi
        done
        if [ -n "$STOP_BARRIER" ]; then
            if [ -f "$STOP_BARRIER.started" ] && [ ! -f "$STOP_BARRIER.finished" ]; then
                printf 'stop overlapped stop\n' >> "$TRACE"
            else
                touch "$STOP_BARRIER.started"
                i=0
                while [ ! -f "$STOP_BARRIER.seen" ] && [ "$i" -lt 40 ]; do
                    i=$((i + 1))
                    sleep 0.02
                done
                touch "$STOP_BARRIER.finished"
            fi
        fi
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
        (self.root / "date").write_text(
            "#!/bin/sh\n"
            "base=${DATE_BASE:-1700000000}\n"
            "offset=0\n"
            "if [ -n \"${DATE_OFFSET_FILE:-}\" ] && [ -f \"$DATE_OFFSET_FILE\" ]; then\n"
            "    offset=$(cat \"$DATE_OFFSET_FILE\")\n"
            "fi\n"
            "echo $((base + offset))\n"
        )
        (self.root / "date").chmod(0o700)
        for name, script in (("compose", COMPOSE), ("docker", DOCKER)):
            stub = self.root / name
            stub.write_text(script)
            stub.chmod(0o700)

    def add_container(self, name, project="", config="", oneoff="False", container_id="owned", after_up=False, service=""):
        path = self.containers / container_id
        path.write_text(f"{name}\t{project}\t{config}\t{oneoff}\t{service}\n")
        if after_up:
            path.with_name(container_id + ".after").write_text("1\n")

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
            VANISHED=str(self.root / "vanished"),
            INSPECT_ONCE_MARK=str(self.root / "inspect-once"),
            DOCKER_PS_ONCE_MARK=str(self.root / "docker-ps-once"),
            DOCKER_PS_COUNT_FILE=str(self.root / "docker-ps-count"),
            CONFIG_ONCE_MARK=str(self.root / "config-once"),
            DATE_OFFSET_FILE=str(self.root / "date-offset"),
            STOP_ONCE_MARK=str(self.root / "stop-once"),
        )
        project_name = settings.pop("PROJECT_NAME", None)
        env.update(settings)
        if model is None:
            model = {name: {} for name in env["SERVICES"].splitlines()}
        document = {"services": model}
        if project_name:
            document["name"] = project_name
        env["CONFIG_JSON"] = json.dumps(document)

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
        model = {
            "app": {"depends_on": {"init": {"condition": "service_completed_successfully"}}},
            "init": {},
        }
        self.action("monitor", 0, model=model, SERVICES="app\ninit", COMPOSE_IDS="init-exited\napp")

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
        model = {
            "app": {"depends_on": {"init": {"condition": "service_completed_successfully"}}},
            "init": {},
        }
        self.action("monitor", 1, model=model, SERVICES="app\ninit", COMPOSE_IDS="app\ninit-failed")

    def test_current_model_overrides_stale_completed_dependency_label(self):
        model = {"app": {}, "init": {}}
        self.add_container("init-1", "my-project", self.yaml, container_id="init-exited")
        self.add_container("app-1", "my-project", self.yaml, container_id="app")
        calls = self.action("monitor", 1, model=model, SERVICES="app\ninit", COMPOSE_IDS="init-exited\napp")
        self.assert_no_mutation(calls)
        calls = self.action(
            "start",
            0,
            model=model,
            SERVICES="app\ninit",
            COMPOSE_IDS="init-exited\napp",
            AFTER_UP_IDS="app\ninit-running",
        )
        self.assertIn("compose -f docker-compose.yaml up -d", calls)

    def test_configured_initializer_can_exit_without_dependency_label(self):
        model = {
            "app": {"depends_on": {"init": {"condition": "service_completed_successfully"}}},
            "init": {},
        }
        self.action("monitor", 0, model=model, SERVICES="app\ninit", COMPOSE_IDS="init-exited\napp-plain")

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

    def test_retired_replica_does_not_make_scaled_service_partial(self):
        model = {"app": {"deploy": {"replicas": 1}}}
        self.assert_no_mutation(self.action("monitor", 0, model=model, COMPOSE_IDS="app\nretired"))
        self.assert_no_mutation(self.action("start", 0, model=model, COMPOSE_IDS="app\nretired"))
        self.action("monitor", 1, model=model, COMPOSE_IDS="app\nrunning")
        self.action("monitor", 0, model=model, COMPOSE_IDS="app\nretired-failed")

    def test_extra_successful_initializer_replica_is_not_surplus(self):
        def model(replicas):
            return {
                "app": {"depends_on": {"init": {"condition": "service_completed_successfully"}}},
                "init": {"deploy": {"replicas": replicas}},
            }

        one = model(1)
        exited = "init-exited\ninit-exited-retired\napp"
        self.assert_no_mutation(self.action("monitor", 0, model=one, SERVICES="app\ninit", COMPOSE_IDS=exited))
        self.assert_no_mutation(self.action("start", 0, model=one, SERVICES="app\ninit", COMPOSE_IDS=exited))
        self.action(
            "monitor",
            1,
            model=one,
            SERVICES="app\ninit",
            COMPOSE_IDS="init-running\ninit-running-2\napp",
        )
        self.action("monitor", 1, model=one, SERVICES="app\ninit", COMPOSE_IDS="app")
        two = model(2)
        self.action(
            "monitor",
            0,
            model=two,
            SERVICES="app\ninit",
            COMPOSE_IDS="init-exited\ninit-exited-retired\ninit-exited-old\napp",
        )
        self.action("monitor", 1, model=two, SERVICES="app\ninit", COMPOSE_IDS="init-exited\napp")

    def test_monitor_compose_failed(self):
        self.action("monitor", 1, COMPOSE_PS_ERROR="9")

    def test_monitor_inspect_failed(self):
        self.action("monitor", 1, COMPOSE_IDS="running", INSPECT_ERROR="9")

    def test_monitor_rechecks_after_listed_container_disappears(self):
        self.add_container("app-run", "my-project", self.yaml, "True", "task")
        settings = dict(COMPOSE_IDS="app\ntask", VANISH_ID="task")
        calls = self.action("monitor", 0, **settings)
        self.assert_no_mutation(calls)
        inspects = [call for call in calls if call.startswith("docker inspect ")]
        self.assertGreaterEqual(len(inspects), 2, calls)
        self.assertIn("task", inspects[0].split())
        self.assertNotIn("task", inspects[-1].split())
        self.assertIn("app", inspects[-1].split())
        self.trace.write_text("")
        (self.root / "vanished").unlink()
        calls = self.action("start", 0, **settings)
        self.assert_no_mutation(calls)

    def test_monitor_refresh_keeps_real_inspect_and_enumeration_failures(self):
        self.add_container("app-run", "my-project", self.yaml, "True", "task")
        self.assert_no_mutation(self.action(
            "monitor", 1, COMPOSE_IDS="app\ntask", VANISH_ID="task", INSPECT_ERROR_AFTER_VANISH="9",
        ))
        self.trace.write_text("")
        (self.root / "vanished").unlink()
        self.assert_no_mutation(self.action(
            "monitor", 1, COMPOSE_IDS="app\ntask", VANISH_ID="task", PS_ERROR_AFTER_VANISH="9",
        ))
        self.trace.write_text("")
        self.action("monitor", 1, COMPOSE_IDS="app", INSPECT_FAIL_ONCE="1")

    def test_monitor_refresh_reevaluates_a_missing_service(self):
        calls = self.action(
            "monitor", 1, SERVICES="app\nworker", COMPOSE_IDS="app\nworker", VANISH_ID="worker",
        )
        self.assert_no_mutation(calls)
        inspects = [call for call in calls if call.startswith("docker inspect ")]
        self.assertGreaterEqual(len(inspects), 2, calls)
        self.assertNotIn("worker", inspects[-1].split())

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
        self.assert_no_mutation(self.action("monitor", 1, SERVICES="app\nhelper", COMPOSE_IDS="helper"))
        self.assert_no_mutation(self.action("start", 1, SERVICES="app\nhelper", COMPOSE_IDS="helper"))

    def test_inactive_former_project_is_not_an_ownership_conflict(self):
        self.add_container("old-matrixone", "old-project", self.yaml, container_id="old")
        (self.root / "trace.old.stopped").touch()
        self.add_container("new-matrixone", "new-project", self.yaml, container_id="running")
        self.assert_no_mutation(self.action("monitor", 0, COMPOSE_IDS="running"))
        self.assert_no_mutation(self.action("start", 0, COMPOSE_IDS="running"))

    def test_inactive_former_project_is_excluded_from_health(self):
        self.add_container("old-init", "old-project", self.yaml, container_id="former-init")
        self.add_container("app-1", "new-project", self.yaml, container_id="app-plain")
        model = {
            "app": {"depends_on": {"init": {"condition": "service_completed_successfully"}}},
            "init": {},
        }
        self.action(
            "monitor",
            0,
            model=model,
            SERVICES="app\ninit",
            COMPOSE_IDS="init-exited\napp-plain",
        )

    def test_start_accepts_replacement_after_former_project_exits(self):
        self.add_container("old-matrixone", "old-project", self.yaml, container_id="old")
        (self.root / "trace.old.stopped").touch()
        self.add_container("new-matrixone", "new-project", self.yaml, container_id="running", after_up=True)
        calls = self.action("start", 0, AFTER_UP_IDS="running")
        self.assertIn("compose -f docker-compose.yaml up -d", calls)

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
        self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)
        self.assert_no_mutation(calls)

    def test_stop_compose_success_with_absolute_yaml(self):
        calls = self.action("stop", 0, COMPOSE_IDS="running", OCF_RESKEY_ymlfile=str(self.yaml))
        self.assertIn(f"compose -f {self.yaml} down --remove-orphans=false", calls)
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

    def test_stop_overlaps_service_and_oneoff_shutdown(self):
        self.add_container("app-task", "my-project", self.yaml, "True", container_id="task")
        calls = self.action("stop", 0, COMPOSE_IDS="running", STOP_BARRIER=str(self.root / "barrier"))
        self.assertIn("docker stop task", calls)
        self.assertIn("down overlapped stop", calls)
        self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)

    def test_stop_overlaps_former_project_service_and_oneoff(self):
        self.add_container("matrixone", "previous-project", self.yaml, container_id="service")
        self.add_container("matrixone-run", "previous-project", self.yaml, "True", "task")
        calls = self.action("stop", 0, STOP_BARRIER=str(self.root / "barrier"))
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        together = any({"service", "task"} <= set(call.split()[2:]) for call in stop_calls)
        self.assertTrue(together or "stop overlapped stop" in calls, calls)
        self.assertIn("down overlapped stop", calls)
        self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
        for call in stop_calls:
            arguments = call.split()[2:]
            self.assertFalse(any(arg == "-t" or arg.startswith("--time") or arg.startswith("-t") for arg in arguments), call)
        self.assertTrue((self.root / "trace.service.stopped").exists())
        self.assertTrue((self.root / "trace.task.stopped").exists())

    def test_stop_leaves_current_services_to_compose_down(self):
        self.add_container("matrixone", "my-project", self.yaml, container_id="matrixone")
        self.add_container("worker", "my-project", self.yaml, container_id="worker")
        self.add_container("mo-run", "my-project", self.yaml, "True", "task")
        calls = self.action(
            "stop",
            0,
            COMPOSE_IDS="matrixone\nworker",
            STOP_BARRIER=str(self.root / "barrier"),
        )
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        self.assertEqual([call.split()[2:] for call in stop_calls], [["task"]], calls)
        self.assertIn("down overlapped stop", calls)
        self.assertNotIn("docker kill matrixone", calls)
        self.assertNotIn("docker kill worker", calls)
        self.assertTrue((self.root / "trace.matrixone.stopped").exists())
        self.assertTrue((self.root / "trace.worker.stopped").exists())
        self.assertTrue((self.root / "trace.task.stopped").exists())

    def test_stop_overlaps_current_and_former_project_services(self):
        self.add_container("matrixone", "renamed", self.yaml, container_id="current")
        self.add_container("old-matrixone", "previous-project", self.yaml, container_id="former")
        self.assert_no_mutation(self.action("monitor", 1, COMPOSE_IDS="current", PROJECT_NAME="renamed"))
        self.assert_no_mutation(self.action("start", 1, COMPOSE_IDS="current", PROJECT_NAME="renamed"))
        self.trace.write_text("")
        calls = self.action(
            "stop",
            0,
            COMPOSE_IDS="current",
            PROJECT_NAME="renamed",
            STOP_BARRIER=str(self.root / "barrier"),
        )
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        self.assertTrue(stop_calls, calls)
        for call in stop_calls:
            arguments = call.split()[2:]
            self.assertIn("former", arguments, call)
            self.assertNotIn("current", arguments, call)
            self.assertFalse(
                any(arg == "-t" or arg.startswith("--time") or arg.startswith("-t") for arg in arguments),
                call,
            )
        self.assertIn("down overlapped stop", calls)
        self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
        self.assertTrue((self.root / "trace.current.stopped").exists())
        self.assertTrue((self.root / "trace.former.stopped").exists())

    def test_stop_overlaps_removed_same_project_service(self):
        self.add_container("matrixone", "my-project", self.yaml, container_id="matrixone", service="app")
        self.add_container("helper", "my-project", self.yaml, container_id="helper", service="helper")
        calls = self.action(
            "stop",
            0,
            SERVICES="app",
            COMPOSE_IDS="matrixone",
            STOP_BARRIER=str(self.root / "barrier"),
        )
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        self.assertEqual([call.split()[2:] for call in stop_calls], [["helper"]], calls)
        self.assertIn("down overlapped stop", calls)
        self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)
        self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
        for call in stop_calls:
            arguments = call.split()[2:]
            self.assertNotIn("matrixone", arguments, call)
            self.assertFalse(
                any(arg == "-t" or arg.startswith("--time") or arg.startswith("-t") for arg in arguments),
                call,
            )
        self.assertTrue((self.root / "trace.matrixone.stopped").exists())
        self.assertTrue((self.root / "trace.helper.stopped").exists())

    def test_stop_retries_listing_so_removed_service_still_overlaps_down(self):
        self.add_container("matrixone", "my-project", self.yaml, container_id="matrixone", service="app")
        self.add_container("helper", "my-project", self.yaml, container_id="helper", service="helper")
        calls = self.action(
            "stop",
            0,
            SERVICES="app",
            COMPOSE_IDS="matrixone",
            DOCKER_PS_FAIL_ONCE="1",
            STOP_BARRIER=str(self.root / "barrier"),
        )
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        self.assertEqual([call.split()[2:] for call in stop_calls], [["helper"]], calls)
        stop_at = calls.index(stop_calls[0])
        self.assertGreaterEqual(sum(call.startswith("docker ps ") for call in calls[:stop_at]), 2, calls)
        self.assertIn("down overlapped stop", calls)
        self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)
        self.assertNotIn("docker stop repeated helper", calls)
        self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
        for call in stop_calls:
            arguments = call.split()[2:]
            self.assertNotIn("matrixone", arguments, call)
            self.assertFalse(
                any(arg == "-t" or arg.startswith("--time") or arg.startswith("-t") for arg in arguments),
                call,
            )
        self.assertTrue((self.root / "trace.matrixone.stopped").exists())
        self.assertTrue((self.root / "trace.helper.stopped").exists())

    def test_stop_does_not_signal_orphan_twice_when_remove_orphans_is_inherited(self):
        self.add_container("matrixone", "my-project", self.yaml, container_id="matrixone", service="app")
        self.add_container("helper", "my-project", self.yaml, container_id="helper", service="helper")
        calls = self.action(
            "stop",
            0,
            SERVICES="app",
            COMPOSE_IDS="matrixone",
            COMPOSE_REMOVE_ORPHANS="true",
            STOP_BARRIER=str(self.root / "barrier"),
        )
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        self.assertEqual([call.split()[2:] for call in stop_calls], [["helper"]], calls)
        self.assertIn("down overlapped stop", calls)
        self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)
        self.assertNotIn("down stop repeated helper", calls)
        self.assertNotIn("docker stop repeated helper", calls)
        self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
        for call in stop_calls:
            arguments = call.split()[2:]
            self.assertNotIn("matrixone", arguments, call)
            self.assertFalse(
                any(arg == "-t" or arg.startswith("--time") or arg.startswith("-t") for arg in arguments),
                call,
            )
        self.assertTrue((self.root / "trace.matrixone.stopped").exists())
        self.assertTrue((self.root / "trace.helper.stopped").exists())

    def test_stop_recovered_model_does_not_stop_current_services_early(self):
        def run(fail_once):
            self.trace.write_text("")
            mark = self.root / "config-once"
            if mark.exists():
                mark.unlink()
            for name in ("trace.matrixone.stopped", "trace.worker.stopped"):
                path = self.root / name
                if path.exists():
                    path.unlink()
            settings = dict(
                SERVICES="app\nworker",
                COMPOSE_IDS="worker\nmatrixone",
                PROJECT_NAME="renamed",
                STOP_BARRIER=str(self.root / "barrier"),
            )
            if fail_once:
                settings["CONFIG_FAIL_ONCE"] = "1"
            calls = self.action("stop", 0, **settings)
            stop_calls = [call for call in calls if call.startswith("docker stop ")]
            for call in stop_calls:
                arguments = call.split()[2:]
                self.assertNotIn("matrixone", arguments, call)
                self.assertNotIn("worker", arguments, call)
                self.assertFalse(
                    any(arg == "-t" or arg.startswith("--time") or arg.startswith("-t") for arg in arguments),
                    call,
                )
            self.assertNotIn("docker stop repeated matrixone", calls)
            self.assertNotIn("docker stop repeated worker", calls)
            self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)
            ordered = [call.split()[-1] for call in calls if call.startswith("down ordered ")]
            self.assertEqual(ordered, ["worker", "matrixone"], calls)
            self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
            self.assertTrue((self.root / "trace.worker.stopped").exists())
            self.assertTrue((self.root / "trace.matrixone.stopped").exists())

        self.add_container("matrixone", "renamed", self.yaml, container_id="matrixone", service="app")
        self.add_container("worker", "renamed", self.yaml, container_id="worker", service="worker")
        run(False)
        (self.root / "stopped").unlink(missing_ok=True)
        run(True)

    def test_stop_retries_model_so_removed_service_overlaps_down(self):
        self.add_container("matrixone", "my-project", self.yaml, container_id="matrixone", service="app")
        self.add_container("helper", "my-project", self.yaml, container_id="helper", service="helper")
        calls = self.action(
            "stop",
            0,
            SERVICES="app",
            COMPOSE_IDS="matrixone",
            CONFIG_FAIL_ONCE="1",
            STOP_BARRIER=str(self.root / "barrier"),
        )
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        self.assertEqual([call.split()[2:] for call in stop_calls], [["helper"]], calls)
        self.assertIn("down overlapped stop", calls)
        self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)
        self.assertNotIn("docker stop repeated helper", calls)
        self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
        self.assertTrue((self.root / "trace.matrixone.stopped").exists())
        self.assertTrue((self.root / "trace.helper.stopped").exists())

    def test_stop_bounds_retired_service_when_listings_fail_until_after_down(self):
        self.add_container("matrixone", "my-project", self.yaml, container_id="matrixone", service="app")
        self.add_container("helper", "my-project", self.yaml, container_id="helper", service="helper")
        calls = self.action(
            "stop",
            0,
            SERVICES="app",
            COMPOSE_IDS="matrixone",
            DOCKER_PS_FAIL_COUNT="2",
            DOWN_ELAPSED="60",
        )
        self.assertNotIn("down overlapped stop", calls)
        self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        self.assertEqual(len(stop_calls), 1, calls)
        down_at = calls.index("compose -f docker-compose.yaml down --remove-orphans=false")
        stop_at = calls.index(stop_calls[0])
        self.assertLess(down_at, stop_at, calls)
        arguments = stop_calls[0].split()[2:]
        self.assertEqual(arguments[:2], ["-t", "50"], calls)
        self.assertEqual(arguments[2:], ["helper"], calls)
        self.assertNotIn("docker stop repeated helper", calls)
        self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
        self.assertTrue((self.root / "trace.matrixone.stopped").exists())
        self.assertTrue((self.root / "trace.helper.stopped").exists())

    def test_stop_bounds_recovery_when_overlapped_stop_fails(self):
        self.add_container("matrixone", "my-project", self.yaml, container_id="matrixone", service="app")
        self.add_container("helper", "my-project", self.yaml, container_id="helper", service="helper")
        calls = self.action(
            "stop",
            0,
            SERVICES="app",
            COMPOSE_IDS="matrixone",
            STOP_FAIL_ONCE="1",
            DOWN_ELAPSED="60",
            STOP_BARRIER=str(self.root / "barrier"),
        )
        self.assertIn("docker stop helper", calls)
        self.assertIn("compose -f docker-compose.yaml down --remove-orphans=false", calls)
        stop_calls = [call for call in calls if call.startswith("docker stop ")]
        self.assertEqual(stop_calls[-1], "docker stop -t 50 helper", calls)
        down_at = calls.index("compose -f docker-compose.yaml down --remove-orphans=false")
        self.assertLess(down_at, calls.index(stop_calls[-1]), calls)
        self.assertNotIn("docker stop repeated helper", calls)
        self.assertFalse(any(call.startswith("docker kill ") for call in calls), calls)
        self.assertTrue((self.root / "trace.matrixone.stopped").exists())
        self.assertTrue((self.root / "trace.helper.stopped").exists())

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
