# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import argparse
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import sirius_campaign as controller
import sirius_campaign_v2 as v2
import sirius_resource as resource
from sirius_campaign_v2_test import provenance, output


class ControllerTest(unittest.TestCase):
    def test_resource_diagnostics_preserve_full_schedule_and_cannot_accept(self):
        from sirius_acceptance_test import evidence

        data = evidence()
        manifest = data[2]
        rows = data[3][176:]
        original = copy.deepcopy(manifest)
        report = controller.resource_diagnostic_summary(manifest, rows, [6, 5])
        self.assertEqual([5, 6], report["resource_phases"])
        self.assertEqual(88, report["validated"])
        self.assertTrue(report["partial"])
        self.assertFalse(report["complete"])
        self.assertEqual(original, manifest)
        with self.assertRaises(v2.CampaignError):
            v2.summarize(manifest, rows)
        for mutation in (
            lambda values: values.pop(),
            lambda values: values[0].update(sequence=1),
            lambda values: values[1].update(query_id=values[0]["query_id"]),
        ):
            changed = copy.deepcopy(rows)
            mutation(changed)
            with self.assertRaises(v2.CampaignError):
                controller.resource_diagnostic_summary(manifest, changed, [5, 6])
        report = controller.resource_diagnostic_summary(
            manifest, data[3], [1, 2, 3, 4, 5, 6]
        )
        self.assertFalse(report["complete"])

    def test_resource_phase_selection_rejects_invalid_or_timing_requests(self):
        self.assertEqual((), controller.resource_phase_selection(False, None))
        self.assertEqual((5, 6), controller.resource_phase_selection(True, [6, 5]))
        for resource, phases in (
            (False, [1]),
            (True, []),
            (True, [1, 1]),
            (True, [0]),
            (True, [7]),
            (True, [True]),
        ):
            with self.assertRaises(v2.CampaignError):
                controller.resource_phase_selection(resource, phases)

    def test_profile_workspace_quota_is_checked_after_export_too(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "trace.sqlite").write_bytes(b"12345")
            self.assertEqual(5, controller.profile_workspace_bytes(root))
            with patch.object(controller, "PROFILE_WORKSPACE_BYTES", 4):
                with self.assertRaisesRegex(v2.CampaignError, "quota"):
                    controller.profile_workspace_bytes(root)

    def test_workspace_scan_tolerates_retired_files_and_keeps_other_failures_fatal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retired = root / "compare-retired.ndjson"
            retained = root / "retained"
            retired.write_bytes(b"old")
            retained.write_bytes(b"12345")
            original = Path.stat

            def retire(path, *args, **kwargs):
                if path == retired:
                    retired.unlink(missing_ok=True)
                    raise FileNotFoundError()
                return original(path, *args, **kwargs)

            with patch.object(Path, "stat", autospec=True, side_effect=retire):
                self.assertEqual(5, controller.profile_workspace_bytes(root))
            with patch.object(controller, "PROFILE_WORKSPACE_BYTES", 4):
                with self.assertRaisesRegex(v2.CampaignError, "quota"):
                    controller.profile_workspace_bytes(root)
            with patch.object(Path, "stat", side_effect=PermissionError()):
                with self.assertRaises(PermissionError):
                    controller.profile_workspace_bytes(root)

    def test_profile_workspace_quota_counts_all_retained_phases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for number in (5, 6):
                phase = root / f"phase-{number}"
                phase.mkdir()
                (phase / "trace.sqlite").write_bytes(b"123")
            with patch.object(controller, "PROFILE_WORKSPACE_BYTES", 4):
                self.assertEqual(
                    3, controller.profile_workspace_bytes(root / "phase-5")
                )
                with self.assertRaisesRegex(v2.CampaignError, "quota"):
                    controller.profile_workspace_bytes(root)

    def test_controller_joins_orphans_after_a_worker_exits(self):
        # The escaped child survives its worker's exit and becomes the outer
        # controller's child. v2's test caller bounds and owns this fixture too.
        script = """import json,subprocess,sys
import sirius_campaign as c
previous=c.v2.legacy._enter_exclusive_subreaper()
worker=subprocess.Popen([sys.executable,'-c',
    'import os,time; pid=os.fork(); '
    'os.setsid() if pid==0 else None; '
    'print(pid,flush=True) if pid else None; '
    'time.sleep(30) if pid==0 else None'],stdout=subprocess.PIPE,text=True)
escaped=int(worker.stdout.readline())
worker.wait(timeout=5)
assert escaped in c.v2.legacy._direct_child_pids()
c.close_services([],previous)
worker.stdout.close()
assert not c.v2.legacy._direct_child_pids()
print(json.dumps({'joined':True}))
"""
        result = v2.BoundedExecutor([sys.executable, "-c", script], 10)({})
        self.assertEqual({"joined": True}, result)

    def test_cleanup_failure_still_joins_descendants_and_keeps_owner_poisoned(self):
        service = MagicMock()
        service.close.side_effect = v2.CampaignError("injected close failure")
        with patch.object(
            v2.legacy, "_kill_and_reap_adopted_children"
        ) as join, patch.object(v2.legacy, "_set_child_subreaper") as restore:
            with self.assertRaisesRegex(v2.CampaignError, "cleanup failed"):
                controller.close_services([service], False)
        join.assert_called_once()
        restore.assert_not_called()

    def test_profile_identity_is_written_before_exec_and_checked_by_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "identity.json"
            arguments = argparse.Namespace(
                identity=path,
                service=Path("/owned/service"),
                configuration=Path("/owned/config"),
            )
            with patch.object(controller, "pid_identity", return_value=7), patch.object(
                controller.os, "execve"
            ) as execute:
                controller.launch_target(arguments)
            identity = json.loads(path.read_text())
            self.assertEqual(7, identity["generation"])
            self.assertEqual(
                ["/owned/service", "-cfg", "/owned/config"], execute.call_args.args[1]
            )
            service = MagicMock(targets={})
            with patch.object(controller, "pid_identity", return_value=7):
                self.assertEqual(identity["pid"], controller.wait_target(service, path))
            self.assertEqual({identity["pid"]: 7}, service.targets)
            service.targets.clear()
            with patch.object(controller, "pid_identity", return_value=8):
                with self.assertRaisesRegex(v2.CampaignError, "generation"):
                    controller.wait_target(service, path)
            self.assertEqual({}, service.targets)
            path.chmod(0o644)
            with self.assertRaisesRegex(v2.CampaignError, "private"):
                controller.wait_target(service, path)

    def test_pool_tags_require_both_phases_and_exact_native_uuid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "sirius.log"
            identity = "a" * 32
            lines = []
            for phase in ("QueryBegin", "QueryEnd"):
                for tier, space, tail in (
                    ("host", -1, "free_blocks=4"),
                    ("gpu", 0, "reserved=0 bytes"),
                ):
                    lines.append(
                        f"[info] [{tier}_pool] {tier.upper()}:{space} {phase} instance=0xabc connection=1 window=3 query=8 allocated=0 bytes peak=9 bytes {tail}\n"
                    )
            lines.insert(2, f"[info] query 3 telemetry_query={identity}\n")
            path.write_text("".join(lines))
            collector = controller.PoolCollector(root)
            result = collector.collect({}, identity)
            self.assertEqual(2, len(result["pools"]))
            self.assertNotIn("instance", json.dumps(result))
            # Quent UUIDs are independent of MO UUIDs; exclusive CN ordering
            # supplies the separate mapping, with complete window accounting.
            self.assertEqual(result, collector.collect({}, "b" * 32, window_id=3))
            with self.assertRaises(v2.CampaignError):
                collector.collect({}, "b" * 32)
            path.write_text("".join(lines[:-1]))
            with self.assertRaisesRegex(v2.CampaignError, "incomplete"):
                collector.collect({}, identity)

    def test_configuration_is_private_at_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "configuration.json"
            controller.private_json(path, {"safe": "data"})
            self.assertEqual(0o600, path.stat().st_mode & 0o777)
            with self.assertRaises(FileExistsError):
                controller.private_json(path, {})

    def test_window_accounting_is_bounded_and_rejects_extra_admissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sirius.log"
            collector = controller.PoolCollector(directory)
            path.write_text(f"query 1 telemetry_query={'a'*32}\n")
            # Match the engine's surrounding log whitespace.
            path.write_text("[info] " + path.read_text())
            collector.validate_windows(1)
            with path.open("a") as file:
                file.write(f"[info] query 2 telemetry_query={'b'*32}\n")
            with self.assertRaisesRegex(v2.CampaignError, "ambiguous"):
                collector.validate_windows(1)
            path.write_bytes(b"x" * ((1 << 20) + 1))
            with self.assertRaisesRegex(v2.CampaignError, "line exceeds"):
                collector.validate_windows(0)

    def test_cleanup_observation_preserves_end_and_uses_next_idle_begin(self):
        with tempfile.TemporaryDirectory() as directory:
            lines = []
            for window in (1, 2):
                for phase in ("QueryBegin", "QueryEnd"):
                    for tier, space, tail in (
                        ("host", -1, "free_blocks=4"),
                        ("gpu", 0, "reserved=0 bytes"),
                    ):
                        amount = (
                            768
                            if (window, phase, tier) == (1, "QueryEnd", "gpu")
                            else 0
                        )
                        lines.append(
                            f"[info] [{tier}_pool] {tier.upper()}:{space} {phase} instance=0xabc connection=1 window={window} query={window} allocated={amount} bytes peak=9 bytes {tail}\n"
                        )
                    if phase == "QueryBegin":
                        lines.append(
                            f"[info] query {window} telemetry_query={'c'*32}\n"
                        )
            (Path(directory) / "sirius.log").write_text("".join(lines))
            executor = controller.PublicExecutor.__new__(controller.PublicExecutor)
            executor.pools = controller.PoolCollector(directory)
            executor.native_ordinal = 2
            executor.pool_offsets = {"a" * 32: ({}, 1), "b" * 32: ({}, 2)}
            rows = [
                {"backend": "embedded", "query_id": value * 32} for value in ("a", "b")
            ]
            executor.attach_pools(rows)
            gpu = next(
                pool
                for pool in rows[0]["native_pools"]["pools"]
                if pool["tier"] == "gpu"
            )
            self.assertEqual(768, gpu["end"]["allocated_bytes"])
            self.assertEqual(0, gpu["after_close"]["allocated_bytes"])
            self.assertEqual("b" * 32, gpu["after_close"]["query_id"])
            self.assertEqual({}, executor.pool_offsets)

    def test_profile_close_never_signals_reused_target_pid(self):
        service = controller.Service.__new__(controller.Service)
        service.targets = {123: 1}
        service.process = MagicMock(pid=456)
        service.process.poll.return_value = None
        service.process.returncode = 0
        service.generation = 3
        service.file = MagicMock()
        with patch.object(
            controller, "pid_identity", side_effect=lambda pid: 2 if pid == 123 else 3
        ), patch.object(controller.os, "kill") as kill, patch.object(
            controller.os, "killpg"
        ) as killpg:
            service.close()
        kill.assert_not_called()
        killpg.assert_not_called()
        service.process.wait.assert_called_once_with(
            timeout=controller.PROFILE_FINALIZE_SECONDS
        )
        service.file.close.assert_called_once()

    def test_cuda_diagnostics_require_the_owned_pid_or_global_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.sqlite"
            db = sqlite3.connect(path)
            db.executescript(
                """create table ENUM_CUDA_MEM_KIND(id integer,label text);
            insert into ENUM_CUDA_MEM_KIND values(1,'Pinned'),(2,'Device');
            create table ENUM_CUDA_DEV_MEM_EVENT_OPER(id integer,label text);
            insert into ENUM_CUDA_DEV_MEM_EVENT_OPER values(0,'Allocation');
            create table CUDA_GPU_MEMORY_USAGE_EVENTS(start integer,globalPid integer,contextId integer,address integer,bytes integer,memKind integer,memoryOperationType integer);
            insert into CUDA_GPU_MEMORY_USAGE_EVENTS values(1,2063597568,0,10,64,1,0),(2,2063597568,0,20,128,2,0);
            create table DIAGNOSTIC_EVENT(severity integer,text text,globalPid integer);
            insert into DIAGNOSTIC_EVENT values(2,'No CUDA events collected',16777216000);"""
            )
            db.commit()
            resource.allocation_summary(path, 123)
            for owner in (123 << 24, None, -1, 0):
                db.execute("update DIAGNOSTIC_EVENT set globalPid=?", (owner,))
                db.commit()
                with self.assertRaisesRegex(v2.CampaignError, "incomplete"):
                    resource.allocation_summary(path, 123)
            db.close()

    def test_structured_events_correlate_exact_uuid_and_account(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cn.log"
            identity = "a" * 32
            schema = {
                "msg": "MO campaign result schema",
                "query_id": identity,
                "account_id": 0,
            }
            terminal = {
                "msg": "Sirius embedded execution",
                "query_id": identity,
                "account_id": 0,
            }
            path.write_text(json.dumps(schema) + "\n" + json.dumps(terminal) + "\n")
            self.assertEqual(
                (schema, terminal), controller.EventCollector(path).collect(0, identity)
            )
            path.write_text(json.dumps(schema) + "\n" + json.dumps(schema) + "\n")
            with self.assertRaisesRegex(v2.CampaignError, "duplicate"):
                controller.EventCollector(path).collect(0, identity)
            schema["account_id"] = 1
            path.write_text(json.dumps(schema) + "\n")
            with self.assertRaisesRegex(v2.CampaignError, "account"):
                controller.EventCollector(path).collect(0, identity)

    def test_metadata_rejection_still_releases_unadmitted_spool(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.chmod(0o700)
            candidate = v2.build_campaign(provenance())
            candidate["schedule"] = candidate["schedule"][:1]
            fake = output(root, "run-00000001-mo-native.ndjson", [1])
            fake.update(
                {
                    "schema_version": 2,
                    "sequence": 1,
                    "route": candidate["schedule"][0]["route"],
                    "query_id": "a" * 32,
                    "wall_seconds": 1.0,
                    "first_row_seconds": 0.1,
                }
            )
            fake["output_schema_sha256"] = "f" * 64
            with self.assertRaises(v2.CampaignError):
                v2.execute_campaign(candidate, lambda _: fake, v2.Scratch(root))
            self.assertEqual([], list(root.iterdir()))

    def test_partial_schedule_cannot_publish_complete_summary(self):
        candidate = v2.build_campaign(provenance())
        candidate["schedule"] = candidate["schedule"][:2]
        with self.assertRaisesRegex(v2.CampaignError, "partial"):
            v2.summarize(candidate, [])

    def test_sampler_streams_large_endpoint_without_materializing(self):
        sampler = controller.Sampler.__new__(controller.Sampler)
        sampler.pid = 1
        sampler.generation = 2
        sampler.metric_url = "owned"
        sampler.rss_peak = 0
        sampler.go_peak = 0
        sampler.samples = 0
        sampler.gpu = False
        stream = io.BytesIO(
            b"# other metric\n" * 100000 + b"go_memstats_heap_alloc_bytes 42\n"
        )
        with patch.object(controller, "pid_identity", return_value=2), patch.object(
            controller, "process_observation", return_value=(0, 3)
        ), patch.object(controller.urllib.request, "urlopen", return_value=stream):
            sampler._sample()
        self.assertEqual(42, sampler.go_peak)
        self.assertEqual(1, sampler.samples)

    def test_allocation_trace_requires_both_sources_and_matching_frees(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.sqlite"
            db = sqlite3.connect(path)
            db.executescript(
                """create table ENUM_CUDA_MEM_KIND(id integer,label text);
            insert into ENUM_CUDA_MEM_KIND values(2,'PINNED'),(3,'DEVICE');
            create table ENUM_CUDA_DEV_MEM_EVENT_OPER(id integer,label text);
            insert into ENUM_CUDA_DEV_MEM_EVENT_OPER values(1,'ALLOC'),(2,'FREE');
            create table CUDA_GPU_MEMORY_USAGE_EVENTS(start integer,globalPid integer,contextId integer,address integer,bytes integer,memKind integer,memoryOperationType integer);"""
            )
            pid = 123
            db.executemany(
                "insert into CUDA_GPU_MEMORY_USAGE_EVENTS values(?,?,?,?,?,?,?)",
                [
                    (1, pid << 24, 0, 10, 64, 2, 1),
                    (2, pid << 24, 1, 20, 128, 3, 1),
                    (3, pid << 24, 0, 10, 64, 2, 2),
                    (4, pid << 24, 1, 20, 128, 3, 2),
                ],
            )
            db.commit()
            result = resource.allocation_summary(path, pid)
            self.assertEqual({"pinned": 64, "device": 128}, result["peak_bytes"])
            self.assertEqual({"pinned": 0, "device": 0}, result["final_bytes"])
            with self.assertRaises(v2.CampaignError):
                resource.allocation_summary(path, 999)
            db.close()

    def test_resource_tracker_spills_at_live_memory_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.sqlite"
            db = sqlite3.connect(path)
            db.executescript(
                """create table ENUM_CUDA_MEM_KIND(id integer,label text);
            insert into ENUM_CUDA_MEM_KIND values(1,'Pinned'),(2,'Device');
            create table ENUM_CUDA_DEV_MEM_EVENT_OPER(id integer,label text);
            insert into ENUM_CUDA_DEV_MEM_EVENT_OPER values(0,'Allocation');
            create table CUDA_GPU_MEMORY_USAGE_EVENTS(start integer,globalPid integer,contextId integer,address integer,bytes integer,memKind integer,memoryOperationType integer);
            insert into CUDA_GPU_MEMORY_USAGE_EVENTS values(1,2063597568,0,10,64,1,0),(2,2063597568,0,20,128,2,0);"""
            )
            db.commit()
            db.close()
            with patch.object(resource, "MAX_LIVE_ALLOCATIONS", 1):
                result = resource.allocation_summary(path, 123)
            self.assertEqual({"pinned": 64, "device": 128}, result["peak_bytes"])
            self.assertEqual([], list(Path(directory).glob("sirius-ledger-*")))

    def test_spilled_allocation_generations_and_quota_keep_cleanup_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = resource.AllocationLedger(Path(directory))
            try:
                with patch.object(resource, "MAX_LIVE_ALLOCATIONS", 1):
                    ledger.add((None, 10, "pinned"), 64)
                    ledger.add((1, 20, "device"), 128)
                self.assertIsNotNone(ledger.database)
                self.assertEqual({}, ledger.memory)
                with self.assertRaisesRegex(v2.CampaignError, "reused"):
                    ledger.add((1, 20, "device"), 1)
                self.assertEqual(128, ledger.release((1, 20, "device")))
                with self.assertRaisesRegex(v2.CampaignError, "matching acquisition"):
                    ledger.release((1, 20, "device"))
                ledger.operations = 4095
                with patch.object(resource, "MAX_LEDGER_BYTES", 1):
                    with self.assertRaisesRegex(v2.CampaignError, "quota"):
                        ledger.add((1, 30, "device"), 1)
            finally:
                ledger.close()
            self.assertEqual([], list(Path(directory).glob("sirius-ledger-*")))

    def test_spilled_ledger_cannot_grow_past_sqlite_page_quota(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = resource.AllocationLedger(Path(directory))
            try:
                with patch.object(resource, "MAX_LIVE_ALLOCATIONS", 1), patch.object(
                    resource, "MAX_LEDGER_BYTES", 8192
                ):
                    with self.assertRaisesRegex(v2.CampaignError, "quota"):
                        for address in range(10000):
                            ledger.add((1, address, "device"), 64)
                    self.assertLessEqual(
                        (Path(ledger.directory.name) / "live.sqlite").stat().st_size,
                        8192,
                    )
                    self.assertLess(ledger.operations, 4096)
            finally:
                ledger.close()
            self.assertEqual([], list(Path(directory).glob("sirius-ledger-*")))

    def test_sqlite_sort_file_limit_cleans_scope_and_restores_os_limits(self):
        limits = {
            kind: resource.posix_resource.getrlimit(kind)
            for kind in (
                resource.posix_resource.RLIMIT_FSIZE,
                resource.posix_resource.RLIMIT_AS,
            )
        }
        old_directory = resource.os.environ.get("SQLITE_TMPDIR")
        old_signal = resource.signal.getsignal(resource.signal.SIGXFSZ)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(resource, "MAX_SORT_FILE_BYTES", 8192):
                with resource.bounded_sqlite_sort(Path(directory)) as scratch:
                    path = scratch / "sort-pma"
                    with self.assertRaises(OSError):
                        path.write_bytes(b"x" * 8193)
                    self.assertLessEqual(path.stat().st_size, 8192)
            self.assertEqual([], list(Path(directory).iterdir()))
        self.assertEqual(old_directory, resource.os.environ.get("SQLITE_TMPDIR"))
        self.assertEqual(old_signal, resource.signal.getsignal(resource.signal.SIGXFSZ))
        for kind, old in limits.items():
            self.assertEqual(old, resource.posix_resource.getrlimit(kind))

    def test_nvml_zero_before_context_is_an_observation(self):
        nvml = controller.NVML.__new__(controller.NVML)
        nvml.handle = controller.ctypes.c_void_p(1)
        nvml.library = MagicMock()
        nvml.library.nvmlDeviceGetComputeRunningProcesses.return_value = 0
        self.assertEqual(0, nvml.used(123))


if __name__ == "__main__":
    unittest.main()
