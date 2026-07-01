#!/usr/bin/env python3

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys


SECRET_RE = re.compile(
    r"(?i)(AKIA[0-9A-Z]{16}|X-Amz-Signature=[^&\s]+|AWSAccessKeyId=[^&\s]+|"
    r"(token|secret|password|credential|authorization|access[_-]?key|session[_-]?token)\s*=\s*[^\s;&]+)"
)
OBJECT_RE = re.compile(r"(?i)(s3|gs|abfs|abfss)://[^\s,;\)]+")
ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def die(message):
    print(f"[iceberg-external] ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def redact(text):
    if text is None:
        return ""

    def redact_object(match):
        value = match.group(0)
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
        return f"<redacted:path:sha256:{digest}>"

    text = SECRET_RE.sub("<redacted>", str(text))
    return OBJECT_RE.sub(redact_object, text)


def case_dir_name(case_id, name):
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", f"{case_id}_{name}".strip("_"))
    return safe[:180] or "case"


def expand_env(value, context):
    if value is None:
        return ""
    missing = []

    def repl(match):
        key = match.group(1)
        if key in context:
            return str(context[key])
        env_value = os.environ.get(key, "").strip()
        if env_value == "":
            missing.append(key)
        return env_value

    rendered = ENV_RE.sub(repl, str(value))
    if missing:
        die(f"scenario {context.get('case_id', '<unknown>')} missing environment variables: {', '.join(sorted(set(missing)))}")
    return rendered


def command_template(engine):
    template = engine.get("command_template", "").strip()
    if template:
        return template
    key = engine.get("command_env", "").strip()
    if not key:
        die(f"engine {engine.get('name', '<unknown>')} requires command_env or command_template")
    template = os.environ.get(key, "").strip()
    if not template:
        die(f"{key} is required for engine {engine.get('name', '<unknown>')}")
    return template


def validate_command_template(engine):
    template = engine.get("command_template", "")
    if template and "{sql}" not in template:
        die(f"engine {engine.get('name', '<unknown>')} command_template must contain {{sql}}")
    if not template and not engine.get("command_env"):
        die(f"engine {engine.get('name', '<unknown>')} requires command_env")


def run_sql(engine_name, template, sql, timeout):
    rendered = template.replace("{sql}", shlex.quote(sql))
    proc = subprocess.run(
        ["sh", "-c", rendered],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    rows = normalize_rows(proc.stdout)
    return {
        "engine": engine_name,
        "raw": proc.stdout,
        "rows": rows,
        "returncode": proc.returncode,
    }


def normalize_rows(raw):
    rows = []
    for line in raw.splitlines():
        text = line.strip()
        if not text:
            continue
        lower = text.lower()
        if lower.startswith(("warning:", "time taken:", "query id", "mysql: [warning]")):
            continue
        if lower.startswith(("spark context", "using spark")):
            continue
        if "fetched " in lower and " row" in lower:
            continue
        rows.append(re.sub(r"\s+", " ", text))
    return rows


def fingerprint(rows, mode):
    material = rows if mode == "ordered" else sorted(rows)
    payload = "\n".join(material).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_scenarios(scenarios):
    if not isinstance(scenarios, list) or not scenarios:
        die("scenario file must contain a non-empty JSON array")
    seen = set()
    for scenario in scenarios:
        case_id = str(scenario.get("case_id") or scenario.get("id") or "").strip()
        if not case_id:
            die("scenario requires case_id or id")
        if case_id in seen:
            die(f"duplicate case_id: {case_id}")
        seen.add(case_id)
        mode = scenario.get("comparison_mode", "ordered")
        if mode not in ("ordered", "unordered"):
            die(f"{case_id} comparison_mode must be ordered or unordered")
        engines = scenario.get("engines")
        if not isinstance(engines, list) or not engines:
            die(f"{case_id} requires non-empty engines array")
        for engine in engines:
            name = str(engine.get("name", "")).strip()
            if not name:
                die(f"{case_id} engine requires name")
            validate_command_template(engine)
            if not engine.get("sql") and not engine.get("actions"):
                die(f"{case_id} engine {name} requires sql or actions")
            if engine.get("expect_error_contains") and not engine.get("sql"):
                die(f"{case_id} engine {name} expect_error_contains requires sql")


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path, value):
    path.write_text(redact(value), encoding="utf-8")


def run_scenario(scenario, report_dir, profile, timeout):
    case_id = str(scenario.get("case_id") or scenario.get("id")).strip()
    name = str(scenario.get("name") or case_id).strip()
    mode = scenario.get("comparison_mode", "ordered")
    context = dict(os.environ)
    context.update({k: v for k, v in scenario.items() if isinstance(v, (str, int, float))})
    context["case_id"] = case_id

    case_dir = report_dir / case_dir_name(case_id, name)
    case_dir.mkdir(parents=True, exist_ok=True)

    engine_reports = []
    status = "success"
    failure_category = ""
    sample_mismatch = []

    for engine in scenario["engines"]:
        engine_name = str(engine["name"]).strip()
        template = command_template(engine)
        raw_sections = []
        for idx, action in enumerate(engine.get("actions", []) or []):
            sql = expand_env(action, context)
            result = run_sql(engine_name, template, sql, timeout)
            raw_sections.append(f"-- action {idx}\n-- sql: {redact(sql)}\n{result['raw']}")
            if result["returncode"] != 0:
                status = "failed"
                failure_category = "action_failed"
                sample_mismatch.append(f"{engine_name} action {idx} failed rc={result['returncode']}")
        rows = []
        expected_error = expand_env(engine.get("expect_error_contains", ""), context) if engine.get("expect_error_contains") else ""
        sql = expand_env(engine.get("sql", ""), context)
        if sql:
            result = run_sql(engine_name, template, sql, timeout)
            raw_sections.append(f"-- query\n-- sql: {redact(sql)}\n{result['raw']}")
            rows = result["rows"]
            if expected_error:
                if result["returncode"] == 0:
                    status = "failed"
                    failure_category = "expected_error_missing"
                    sample_mismatch.append(f"{engine_name} query succeeded but expected error containing {redact(expected_error)}")
                elif expected_error not in result["raw"]:
                    status = "failed"
                    failure_category = "expected_error_mismatch"
                    sample_mismatch.append(f"{engine_name} query error did not contain {redact(expected_error)}")
                else:
                    rows = [f"EXPECTED_ERROR {expected_error}"]
            elif result["returncode"] != 0:
                status = "failed"
                failure_category = "query_failed"
                sample_mismatch.append(f"{engine_name} query failed rc={result['returncode']}")
        write_text(case_dir / f"{engine_name}.out", "\n\n".join(raw_sections))
        engine_reports.append({
            "engine": engine_name,
            "row_count": len(rows),
            "checksum": fingerprint(rows, mode),
            "expected_error": bool(expected_error),
            "rows": rows,
        })

    if status == "success" and len(engine_reports) > 1:
        comparable = [report for report in engine_reports if not report.get("expected_error")]
        baseline = comparable[0] if comparable else None
        for other in comparable[1:]:
            if baseline and other["checksum"] != baseline["checksum"]:
                status = "failed"
                failure_category = "data_mismatch"
                sample_mismatch.append(f"{baseline['engine']} checksum {baseline['checksum']} != {other['engine']} checksum {other['checksum']}")
                break

    metadata = {
        "report_schema_version": "iceberg-external-v1",
        "profile": profile,
        "case_id": case_id,
        "scenario": name,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "catalog": scenario.get("catalog", ""),
        "namespace": scenario.get("namespace", ""),
        "table": scenario.get("table", ""),
        "ref": scenario.get("ref", ""),
        "comparison_mode": mode,
        "resolved_snapshot_id": scenario.get("resolved_snapshot_id", ""),
        "metadata_location_hash": hashlib.sha256(str(scenario.get("metadata_location", "")).encode("utf-8")).hexdigest() if scenario.get("metadata_location") else "",
        "status": status,
        "failure_category": failure_category,
    }
    diff = {
        "case_id": case_id,
        "status": status,
        "failure_category": failure_category,
        "engines": [{k: v for k, v in report.items() if k != "rows"} for report in engine_reports],
        "sample_mismatch": sample_mismatch[:10],
    }
    summary_lines = [
        f"# {case_id} {name}",
        "",
        f"- profile: `{profile}`",
        f"- status: `{status}`",
        f"- comparison_mode: `{mode}`",
        "",
        "| engine | row_count | checksum |",
        "| --- | ---: | --- |",
    ]
    for report in engine_reports:
        summary_lines.append(f"| `{report['engine']}` | {report['row_count']} | `{report['checksum']}` |")
    if sample_mismatch:
        summary_lines.extend(["", "## Sample Mismatch", ""])
        summary_lines.extend(f"- {redact(item)}" for item in sample_mismatch[:10])
    write_json(case_dir / "metadata.json", metadata)
    write_json(case_dir / "diff.json", diff)
    write_text(case_dir / "summary.md", "\n".join(summary_lines) + "\n")
    return status


def main():
    parser = argparse.ArgumentParser(description="Run or validate MatrixOne Iceberg external profile scenarios.")
    parser.add_argument("--scenario-file", required=True)
    parser.add_argument("--report-dir", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("MO_ICEBERG_EXTERNAL_TIMEOUT", "1200")))
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    scenario_path = pathlib.Path(args.scenario_file)
    try:
        scenarios = json.loads(scenario_path.read_text(encoding="utf-8"))
    except Exception as exc:
        die(f"read/parse {scenario_path}: {exc}")
    validate_scenarios(scenarios)
    if args.validate_only:
        print(f"[iceberg-external] validated {len(scenarios)} scenarios from {scenario_path}")
        return

    report_dir = pathlib.Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    statuses = [run_scenario(scenario, report_dir, args.profile, args.timeout) for scenario in scenarios]
    if any(status != "success" for status in statuses):
        die(f"{statuses.count('failed')} external profile scenario(s) failed")
    print(f"[iceberg-external] wrote {len(scenarios)} scenario report(s) under {report_dir}")


if __name__ == "__main__":
    main()
