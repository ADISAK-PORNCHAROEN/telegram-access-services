"""Build redacted summaries from terminal performance events and a run manifest."""

from collections import Counter, defaultdict
import json
import math
import re
from statistics import median

REQUIRED_CASES = (
    "small-single",
    "medium-single",
    "large-single",
    "mixed-album",
    "concurrent-users",
)
_LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_. -]{0,63}$")
_JOB_REF = re.compile(r"^[0-9a-fA-F]{8,32}$")
_PROFILE_VALUE = re.compile(r"^[A-Za-z0-9_.+ -]{1,80}$")
_CONTEXT_KEYS = {"account_alias", "datacenter", "network", "host"}
_PROFILE_OBSERVATIONS = {
    "album_strategy", "pipeline_fallback_reason", "pipeline_reserved_bytes",
    "pipeline_peak_buffered_bytes", "workspace_payload_peak_bytes",
}
_STATUSES = {"ok", "partial", "fail", "rejected", "timeout", "cancelled",
             "expired", "skipped: filter", "skipped: too-big", "missing", "text"}


def build_report(events: list[dict], manifest: dict) -> dict:
    """Summarize only allowlisted manifest fields; never copy source events through."""
    clean_manifest = _validate_manifest(manifest)
    by_job: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        if (isinstance(event, dict) and event.get("schema_version") == 1
                and event.get("event") == "copy.terminal"
                and isinstance(event.get("job_id"), str)
                and re.fullmatch(r"[0-9a-fA-F]{32}", event["job_id"])):
            by_job[event["job_id"].lower()].append(event)

    unique = {job_id: values[0] for job_id, values in by_job.items() if len(values) == 1}
    duplicated = {job_id for job_id, values in by_job.items() if len(values) != 1}
    resolved: list[tuple[dict, dict | None, str | None]] = []
    used_refs: set[str] = set()
    assigned_jobs: set[str] = set()
    for run in clean_manifest["runs"]:
        ref = run["job_ref"].lower()
        if ref in used_refs:
            raise ValueError("manifest job references must be unique")
        used_refs.add(ref)
        matches = [job_id for job_id in set(unique) | duplicated if job_id.startswith(ref)]
        if len(matches) != 1:
            resolved.append((run, None, "ambiguous" if matches else "missing"))
        elif matches[0] in duplicated:
            resolved.append((run, None, "ambiguous"))
        else:
            if matches[0] in assigned_jobs:
                raise ValueError("manifest references must identify distinct terminal jobs")
            assigned_jobs.add(matches[0])
            resolved.append((run, unique[matches[0]], None))

    case_runs: dict[str, list[tuple[dict, dict | None, str | None]]] = defaultdict(list)
    for row in resolved:
        case_runs[row[0]["case"]].append(row)

    case_names = sorted(set(REQUIRED_CASES) | set(case_runs))
    summaries = [_summarize_case(name, case_runs[name]) for name in case_names]
    by_case = {case["case"]: case for case in summaries}
    profile_signatures = {
        signature
        for run, event, error in resolved
        if run["case"] in REQUIRED_CASES and event is not None and error is None
        and _number(event.get("total_wall_s"))
        and (signature := _profile_signature(event)) is not None
    }
    profile_consistent = len(profile_signatures) == 1
    complete = (
        clean_manifest["evidence"] == "live"
        and clean_manifest["build_id"] is not None
        and profile_consistent
        and all(
            by_case[required]["matched_runs"] >= 3
            and by_case[required]["total_wall_s"] is not None
            and by_case[required]["total_wall_s"]["samples"] >= 3
            and by_case[required]["profiled_timed_runs"] >= 3
            for required in REQUIRED_CASES
        )
    )
    return {
        "schema_version": 1,
        "report": "telegram-transfer-baseline",
        "run_id": clean_manifest["run_id"],
        "evidence": clean_manifest["evidence"],
        "build_id": clean_manifest.get("build_id"),
        "context": clean_manifest["context"],
        "profile_consistent": profile_consistent,
        "complete_live_baseline": complete,
        "required_cases": list(REQUIRED_CASES),
        "cases": summaries,
        "run_outcomes": [
            {"case": run["case"], "run": run["run"],
             "status": (_safe_status(event.get("status")) if event is not None else unresolved)}
            for run, event, unresolved in resolved
        ],
        "notes": [
            "Stage timings may overlap; use total_wall_s for end-to-end latency.",
            "Offline and synthetic records do not establish Telegram network throughput.",
            "Upload-attempt, retry, cooldown and retransferred-byte values remain unknown when absent.",
            "Event-loop lag is the maximum timer callback delay during active work; short jobs may have no samples.",
            "Workspace payload is logical file bytes, not filesystem allocated space.",
            "Job references and source identifiers are omitted from this report.",
        ],
    }


def render_markdown(report: dict) -> str:
    """Render the same redacted summary as a human-readable Markdown report."""
    lines = [
        "# Telegram transfer baseline",
        "",
        f"- Run: `{report['run_id']}`",
        f"- Evidence: **{report['evidence']}**",
        f"- Build label: `{report['build_id'] or 'not supplied'}`",
        f"- One software/settings profile across required cases: **{'yes' if report['profile_consistent'] else 'no'}**",
        f"- Complete live baseline: **{'yes' if report['complete_live_baseline'] else 'no'}**",
        "",
        "| Case | Declared | Matched | OK | Partial | Failed | Missing/ambiguous | Wall | Queue | Delivered MiB/s |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for case in report["cases"]:
        wall = _format_metric(case["total_wall_s"], "s")
        queue = _format_metric(case["queue_wait_s"], "s")
        throughput = _format_metric(case["delivered_mib_s"], "MiB/s")
        unresolved = case["missing_runs"] + case["ambiguous_runs"]
        lines.append(
            f"| {case['case']} | {case['declared_runs']} | {case['matched_runs']} | "
            f"{case['ok_runs']} | {case['partial_runs']} | {case['failed_runs']} | "
            f"{unresolved} | {wall} | {queue} | {throughput} |"
        )
    if report["context"]:
        lines.extend(["", "## Run context", ""])
        lines.extend(f"- {key}: `{value}`" for key, value in sorted(report["context"].items()))
    for case in report["cases"]:
        lines.extend(["", f"## Case details: {case['case']}", ""])
        lines.append(
            f"- Bytes: downloaded {_format_metric(case['downloaded_bytes'], 'bytes')}; "
            f"delivered {_format_metric(case['delivered_bytes'], 'bytes')}."
        )
        lines.append(
            f"- Retries: {_format_metric(case['retry_attempts'], 'attempts')}; "
            f"coordinator delay {_format_metric(case['retry_delay_s'], 's')}; "
            f"SDK backoff {_format_metric(case['sdk_backoff_wait_s'], 's')}; "
            f"Telegram cooldown {_format_metric(case['cooldown_wait_s'], 's')}."
        )
        lines.append(
            f"- Upload attempts {_format_metric(case['upload_attempts'], 'attempts')} "
            f"({case['upload_attempts_unknown_runs']} unknown runs); "
            f"uploaded bytes {_format_metric(case['uploaded_bytes'], 'bytes')}; "
            f"retransferred bytes {_format_metric(case['retransferred_bytes'], 'bytes')} "
            f"({case['retransferred_bytes_unknown_runs']} unknown runs)."
        )
        lines.append(
            f"- Peak in-flight requests {_format_metric(case['peak_inflight_requests'], 'requests')} "
            f"({case['peak_inflight_unknown_runs']} unknown runs); "
            f"pipeline buffered {_format_metric(case['pipeline_peak_buffered_bytes'], 'bytes')}; "
            f"reserved {_format_metric(case['pipeline_reserved_bytes'], 'bytes')}."
        )
        lines.append(
            f"- Event-loop lag (per-job maximum) {_format_metric(case['event_loop_lag_max_s'], 's')} "
            f"({case['event_loop_lag_unknown_runs']} jobs without samples; "
            f"sample counts {_format_metric(case['event_loop_lag_sample_count'], 'samples')}); "
            f"workspace payload peak {_format_metric(case['workspace_payload_peak_bytes'], 'bytes')}."
        )
        if case["album_strategies"]:
            strategy_text = ", ".join(
                f"{strategy}={count}" for strategy, count in case["album_strategies"].items())
            lines.append(f"- Album strategies: {strategy_text}.")
        if case["pipeline_fallback_reasons"]:
            reason_text = ", ".join(
                f"{reason}={count}"
                for reason, count in case["pipeline_fallback_reasons"].items())
            lines.append(f"- Pipeline fallbacks: {reason_text}.")
        if case["stages_s"]:
            stage_text = "; ".join(
                f"{stage}: {_format_metric(metric, 's')}"
                for stage, metric in case["stages_s"].items())
            lines.append(f"- Stage activity (may overlap): {stage_text}.")
        if case["fallbacks"]:
            fallback_text = ", ".join(
                f"{name}={count}" for name, count in case["fallbacks"].items())
            lines.append(f"- Fallbacks: {fallback_text}.")
        if case["software_profiles"]:
            lines.append("- Profiles:")
            lines.extend(
                f"  - software `{json.dumps(profile['software'], sort_keys=True)}`; "
                f"settings `{json.dumps(profile['effective_settings'], sort_keys=True)}`"
                for profile in case["software_profiles"]
            )
    lines.extend(["", "## Measurement notes", ""])
    lines.extend(f"- {note}" for note in report["notes"])
    lines.append("")
    return "\n".join(lines)


def _validate_manifest(manifest: dict) -> dict:
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be a JSON object")
    allowed = {"run_id", "evidence", "build_id", "context", "runs"}
    if set(manifest) - allowed:
        raise ValueError("manifest contains unsupported fields")
    run_id = _label(manifest.get("run_id"), "run_id")
    evidence = manifest.get("evidence", "offline")
    if not isinstance(evidence, str) or evidence not in {"offline", "live"}:
        raise ValueError("evidence must be offline or live")
    build_id = manifest.get("build_id")
    if build_id is not None:
        build_id = _label(build_id, "build_id")
    context = manifest.get("context", {})
    if not isinstance(context, dict) or set(context) - _CONTEXT_KEYS:
        raise ValueError("context contains unsupported fields")
    clean_context = {}
    for key, value in context.items():
        if key == "datacenter":
            if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,3}", value):
                raise ValueError("datacenter must be a numeric label")
            clean_context[key] = value
        else:
            clean_context[key] = _label(value, key)
    runs = manifest.get("runs")
    if not isinstance(runs, list) or not runs:
        raise ValueError("manifest runs must be a non-empty list")
    clean_runs = []
    seen_case_runs = set()
    for run in runs:
        if not isinstance(run, dict) or set(run) != {"case", "run", "job_ref"}:
            raise ValueError("each run requires only case, run and job_ref")
        case = _label(run["case"], "case")
        number = run["run"]
        ref = run["job_ref"]
        if isinstance(number, bool) or not isinstance(number, int) or number < 1:
            raise ValueError("run number must be a positive integer")
        if not isinstance(ref, str) or not _JOB_REF.fullmatch(ref):
            raise ValueError("job_ref must be 8 to 32 hexadecimal characters")
        key = (case, number)
        if key in seen_case_runs:
            raise ValueError("case/run pairs must be unique")
        seen_case_runs.add(key)
        clean_runs.append({"case": case, "run": number, "job_ref": ref})
    return {"run_id": run_id, "evidence": evidence, "build_id": build_id,
            "context": clean_context, "runs": clean_runs}


def _label(value, field):
    if not isinstance(value, str) or not _LABEL.fullmatch(value):
        raise ValueError(f"{field} must be a short plain-text label")
    return value


def _summarize_case(name, rows):
    events = [event for _run, event, error in rows if event is not None and error is None]
    statuses = Counter(_safe_status(event.get("status")) for event in events)
    timings = [event.get("total_wall_s") for event in events]
    delivered = [event.get("delivered_bytes") for event in events]
    rates = []
    for event in events:
        seconds, count = event.get("total_wall_s"), event.get("delivered_bytes")
        if _number(seconds) and _number(count) and seconds > 0:
            rates.append(count / seconds / (1024**2))
    stage_maps = [_safe_stage_map(event.get("stages_s")) for event in events]
    stages = sorted({stage for stage_map in stage_maps for stage in stage_map})
    retry_values = [event.get("retry_attempts") for event in events]
    retry_delay_values = [event.get("retry_delay_s") for event in events]
    sdk_backoff_values = [event.get("sdk_backoff_wait_s") for event in events]
    upload_attempt_values = [event.get("upload_attempts") for event in events]
    cooldown_values = [event.get("cooldown_wait_s") for event in events]
    inflight_values = [event.get("peak_inflight_requests") for event in events]
    loop_lag_values = [_event_loop_lag_max(event) for event in events]
    loop_lag_sample_values = [
        value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None
        for value in (event.get("event_loop_lag_sample_count") for event in events)]
    pipeline_peak_values = [
        _observed_value(event, "pipeline_peak_buffered_bytes") for event in events]
    pipeline_reserved_values = [
        _observed_value(event, "pipeline_reserved_bytes") for event in events]
    workspace_payload_values = [
        _observed_value(event, "workspace_payload_peak_bytes") for event in events]
    album_strategies = Counter(
        strategy for event in events
        if (strategy := _safe_label(_observed_value(event, "album_strategy"))) is not None)
    pipeline_fallbacks = Counter(
        reason for event in events
        if (reason := _safe_label(
            _observed_value(event, "pipeline_fallback_reason"))) is not None)
    profiled_timed_runs = sum(
        bool(_number(event.get("total_wall_s"))
             and _safe_profile(event.get("software"))
             and _safe_profile(event.get("effective_settings")))
        for event in events)
    fallbacks = Counter()
    for event in events:
        if isinstance(event.get("fallbacks"), dict):
            fallbacks.update({key: value for key, value in event["fallbacks"].items()
                              if isinstance(key, str) and _LABEL.fullmatch(key)
                              and isinstance(value, int) and not isinstance(value, bool)
                              and value >= 0})
    return {
        "case": name,
        "declared_runs": len(rows),
        "matched_runs": len(events),
        "ok_runs": statuses["ok"],
        "partial_runs": statuses["partial"],
        "failed_runs": sum(count for status, count in statuses.items()
                            if status not in {"ok", "partial"}),
        "profiled_timed_runs": profiled_timed_runs,
        "missing_runs": sum(error == "missing" for _run, _event, error in rows),
        "ambiguous_runs": sum(error == "ambiguous" for _run, _event, error in rows),
        "total_wall_s": _metric(timings),
        "queue_wait_s": _metric([event.get("queue_wait_s") for event in events]),
        "downloaded_bytes": _metric([event.get("downloaded_bytes") for event in events]),
        "delivered_bytes": _metric(delivered),
        "delivered_mib_s": _metric(rates),
        "stages_s": {stage: _metric([stage_map.get(stage) for stage_map in stage_maps])
                     for stage in stages},
        "retry_attempts": _metric(retry_values),
        "retry_unknown_runs": sum(value is None for value in retry_values),
        "retry_delay_s": _metric(retry_delay_values),
        "retry_delay_unknown_runs": sum(value is None for value in retry_delay_values),
        "sdk_backoff_wait_s": _metric(sdk_backoff_values),
        "sdk_backoff_unknown_runs": sum(value is None for value in sdk_backoff_values),
        "upload_attempts": _metric(upload_attempt_values),
        "upload_attempts_unknown_runs": sum(value is None for value in upload_attempt_values),
        "cooldown_wait_s": _metric(cooldown_values),
        "cooldown_unknown_runs": sum(value is None for value in cooldown_values),
        "uploaded_bytes": _metric([event.get("uploaded_bytes") for event in events]),
        "uploaded_bytes_unknown_runs": sum(event.get("uploaded_bytes") is None for event in events),
        "retransferred_bytes": _metric([event.get("retransferred_bytes") for event in events]),
        "retransferred_bytes_unknown_runs": sum(
            event.get("retransferred_bytes") is None for event in events),
        "peak_inflight_requests": _metric(inflight_values),
        "peak_inflight_unknown_runs": sum(not _number(value) for value in inflight_values),
        "event_loop_lag_max_s": _metric(loop_lag_values),
        "event_loop_lag_unknown_runs": sum(value is None for value in loop_lag_values),
        "event_loop_lag_sample_count": _metric(loop_lag_sample_values),
        "pipeline_peak_buffered_bytes": _metric(pipeline_peak_values),
        "pipeline_reserved_bytes": _metric(pipeline_reserved_values),
        "workspace_payload_peak_bytes": _metric(workspace_payload_values),
        "album_strategies": dict(sorted(album_strategies.items())),
        "pipeline_fallback_reasons": dict(sorted(pipeline_fallbacks.items())),
        "fallbacks": dict(sorted(fallbacks.items())),
        "software_profiles": _profiles(events),
    }


def _profiles(events):
    profiles = set()
    for event in events:
        software = _safe_profile(event.get("software"))
        settings = _stable_settings(event)
        profiles.add((tuple(sorted((str(k), str(v)) for k, v in software.items())),
                      tuple(sorted((str(k), str(v)) for k, v in settings.items()))))
    return [{"software": dict(software), "effective_settings": dict(settings)}
            for software, settings in sorted(profiles)]


def _profile_signature(event):
    software = _safe_profile(event.get("software"))
    settings = _stable_settings(event)
    if not software or not settings:
        return None
    return (tuple(sorted(software.items())), tuple(sorted(settings.items())))


def _stable_settings(event):
    return {key: value for key, value in _safe_profile(
        event.get("effective_settings")).items() if key not in _PROFILE_OBSERVATIONS}


def _observed_value(event, key):
    """Read new top-level observations and legacy v1 values in effective_settings."""
    value = event.get(key)
    if value is not None:
        return value
    settings = event.get("effective_settings")
    return settings.get(key) if isinstance(settings, dict) else None


def _safe_label(value):
    return value if isinstance(value, str) and _LABEL.fullmatch(value) else None


def _event_loop_lag_max(event):
    samples = event.get("event_loop_lag_sample_count")
    if not isinstance(samples, int) or isinstance(samples, bool) or samples < 1:
        return None
    value = event.get("event_loop_lag_max_s")
    return value if _number(value) else None


def _metric(values):
    usable = [value for value in values if _number(value)]
    if not usable:
        return None
    return {"samples": len(usable), "median": median(usable),
            "min": min(usable), "max": max(usable)}


def _number(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def _safe_profile(value):
    if not isinstance(value, dict):
        return {}
    return {str(key): str(item) for key, item in value.items()
            if isinstance(key, str) and _LABEL.fullmatch(key)
            and isinstance(item, (str, int, float)) and not isinstance(item, bool)
            and _PROFILE_VALUE.fullmatch(str(item))}


def _safe_status(value):
    return value if isinstance(value, str) and value in _STATUSES else "unknown"


def _safe_stage_map(value):
    if not isinstance(value, dict):
        return {}
    return {stage: duration for stage, duration in value.items()
            if isinstance(stage, str) and _LABEL.fullmatch(stage) and _number(duration)}


def _format_metric(metric, unit):
    if not metric:
        return "unknown"
    return (f"{metric['median']:.3f} ({metric['min']:.3f}–{metric['max']:.3f}) "
            f"{unit}, n={metric['samples']}")
