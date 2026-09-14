"""Streamlit dashboard for the AIOps prototype.

Reads enriched findings from the reasoning worker and presents the detector's
findings, evidence, and LLM recommendations. When Prometheus is unreachable the
detector serves sample records and the dashboard shows a FALLBACK banner.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

import api_client as api

REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "15"))
SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"]

st.set_page_config(page_title="AIOps Console", page_icon=None, layout="wide")

st.title("AIOps Console")
st.caption(
    "Robust microservice and cluster AIOps prototype: detector -> async reasoning worker -> dashboard."
)

with st.sidebar:
    st.header("Controls")
    auto_refresh = st.toggle("Auto refresh", value=True, help=f"Refreshes every {REFRESH_SECONDS}s")
    severities = st.multiselect(
        "Severity",
        options=SEVERITY_ORDER,
        default=["critical", "high", "medium"],
    )
    kind_label = st.selectbox("Target kind", options=["all", "app", "cluster"], index=0)
    if st.button("Refresh now", width="stretch"):
        st.rerun()
    if st.button("Run detector now", width="stretch"):
        try:
            result = api.run_analysis()
            st.success(f"Detector ran: {result.get('findings', 0)} new finding(s), source={result.get('source')}.")
        except api.ServiceError as exc:
            st.error(f"Could not reach the detector: {exc}")
    st.divider()
    st.caption(f"Worker: `{api.WORKER_URL}`")
    st.caption(f"Detector: `{api.DETECTOR_URL}`")


def _flatten_metrics(metrics: dict) -> dict:
    flat: dict = {}
    for key, value in (metrics or {}).items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat[f"{key}.{sub_key}"] = sub_value
        else:
            flat[key] = value
    return flat


def _severity_counts(findings: list[dict]) -> dict:
    counts = {level: 0 for level in SEVERITY_ORDER}
    for finding in findings:
        counts[finding["severity"]] = counts.get(finding["severity"], 0) + 1
    return counts


def _render_source_banners(findings: list[dict], status: dict, detector: dict | None) -> None:
    sources = {finding.get("source") for finding in findings}
    detector_source = (detector or {}).get("last_source")

    col_left, col_right = st.columns(2)
    with col_left:
        if "fallback" in sources or (not findings and detector_source == "fallback"):
            st.warning(
                "FALLBACK SAMPLE DATA: Prometheus is not reachable. Findings were produced "
                "from bundled sample records and are for demo purposes."
            )
        elif "prometheus" in sources or detector_source == "prometheus":
            st.success("LIVE DATA: findings were produced from the Prometheus connection.")
        else:
            st.info("No telemetry collected yet. The detector runs on a 3-minute interval.")
    with col_right:
        llm = status.get("llm", {})
        if llm.get("configured"):
            st.success(f"LLM configured: {llm.get('model')} (reason for severity >= {llm.get('reason_min_severity')}).")
        else:
            st.warning(
                "NVIDIA_API_KEY is not configured. Findings still appear, enriched with the "
                "deterministic recommendation catalog."
            )


def _render_detector_info(detector: dict | None) -> None:
    if not detector:
        st.caption("Detector status unavailable.")
        return
    cols = st.columns(4)
    cols[0].metric("Detector runs", detector.get("runs", 0))
    cols[1].metric("Last source", str(detector.get("last_source", "n/a")).upper())
    cols[2].metric("Poll interval", f"{detector.get('poll_interval_seconds', 0)}s")
    cols[3].metric("Series collected", detector.get("last_series_count", 0))
    if detector.get("last_error"):
        st.caption(f"Prometheus fallback reason: {detector['last_error']}")
    st.caption(f"Last analysis: {detector.get('last_run_at')}")


def _render_ai(finding: dict) -> None:
    ai = finding.get("ai", {})
    status = ai.get("status", "pending")

    st.subheader("AI analysis and recommendation")
    if status == "pending":
        st.info("Reasoning in progress. The async worker is calling the LLM.")
        return
    if status == "failed":
        st.warning(f"LLM reasoning failed; deterministic recommendation shown. Error: {ai.get('error')}")
    elif status == "skipped":
        st.info("Below the reasoning severity threshold; deterministic recommendation attached.")

    if ai.get("summary"):
        st.markdown(f"**Summary.** {ai['summary']}")
    if ai.get("root_cause"):
        st.markdown(f"**Likely root cause.** {ai['root_cause']}")
    if ai.get("remediation_steps"):
        st.markdown("**Recommended remediation steps**")
        for index, step in enumerate(ai["remediation_steps"], start=1):
            st.markdown(f"{index}. {step}")
    if ai.get("risk"):
        st.markdown(f"**Risk of acting.** {ai['risk']}")

    confidence = ai.get("confidence")
    if confidence is not None:
        st.progress(min(1.0, max(0.0, float(confidence))), text=f"Confidence: {float(confidence):.0%}")
    st.caption(
        f"provider={ai.get('provider')} model={ai.get('model')} "
        f"latency={ai.get('latency_seconds')}s generated_at={ai.get('generated_at')}"
    )

    if st.button("Re-analyze with LLM", key=f"reanalyze-{finding['incident_id']}"):
        try:
            api.reanalyze(finding["incident_id"])
            st.success("Queued for re-analysis. Refresh in a few seconds.")
        except api.ServiceError as exc:
            st.error(str(exc))


def _render_detail(finding: dict) -> None:
    st.divider()
    header = st.columns([3, 1])
    header[0].subheader(f"{finding['target']}  -  {finding['anomaly']}")
    header[1].metric("Severity", finding["severity"].upper())

    meta = st.columns(4)
    meta[0].metric("Kind", finding["kind"])
    meta[1].metric("Cluster", finding.get("cluster") or "n/a")
    meta[2].metric("Source", str(finding.get("source", "")).upper())
    meta[3].metric("Status", finding.get("status", ""))

    st.markdown(f"**Detector reason.** {finding.get('reason', '')}")
    st.caption(f"incident_id={finding['incident_id']}  created_at={finding['created_at']}  detected_by={finding.get('detected_by')}")

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Evidence")
        evidence = finding.get("evidence", {})
        st.json(evidence)
        flat = _flatten_metrics(evidence.get("metrics", {}))
        if flat:
            st.dataframe(pd.DataFrame(sorted(flat.items()), columns=["metric", "value"]), hide_index=True, width="stretch")
    with right:
        _render_ai(finding)


def _render_content() -> None:
    try:
        status = api.get_status()
        detector = api.detector_status()
        severity_param = ",".join(severities) if severities else None
        kind_param = None if kind_label == "all" else kind_label
        findings = api.get_findings(severity=severity_param, kind=kind_param)
    except api.ServiceError as exc:
        st.error(f"Could not reach the reasoning worker: {exc}")
        st.info("Start the prototype with `docker compose up --build`, or run the services locally.")
        return

    _render_source_banners(findings, status, detector)
    _render_detector_info(detector)

    counts = _severity_counts(findings)
    kpis = st.columns(5)
    kpis[0].metric("Findings", len(findings))
    kpis[1].metric("Critical", counts.get("critical", 0))
    kpis[2].metric("High", counts.get("high", 0))
    kpis[3].metric("Medium", counts.get("medium", 0))
    kpis[4].metric("Reasoning queue", status.get("queue_size", 0))

    if not findings:
        st.info("No findings for the current filters. Use 'Run detector now' to trigger an analysis.")
        return

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.subheader("Findings by severity")
        sev_df = pd.DataFrame(
            {"severity": SEVERITY_ORDER, "count": [counts.get(level, 0) for level in SEVERITY_ORDER]}
        )
        st.bar_chart(sev_df, x="severity", y="count")
    with chart_cols[1]:
        st.subheader("Findings by cluster")
        cluster_counts: dict[str, int] = {}
        for finding in findings:
            key = finding.get("cluster") or "unassigned"
            cluster_counts[key] = cluster_counts.get(key, 0) + 1
        cluster_df = pd.DataFrame(sorted(cluster_counts.items()), columns=["cluster", "count"])
        st.bar_chart(cluster_df, x="cluster", y="count")

    st.subheader("Findings")
    table = pd.DataFrame(
        [
            {
                "incident_id": finding["incident_id"],
                "severity": finding["severity"],
                "kind": finding["kind"],
                "target": finding["target"],
                "cluster": finding.get("cluster"),
                "anomaly": finding["anomaly"],
                "source": finding["source"],
                "status": finding["status"],
                "ai": finding.get("ai", {}).get("status"),
                "created_at": finding["created_at"],
            }
            for finding in findings
        ]
    )
    event = st.dataframe(
        table,
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
    )

    selected_rows = event.selection.rows if event and event.selection else []
    if selected_rows:
        _render_detail(findings[selected_rows[0]])
    else:
        st.caption("Select a row to see evidence and the AI recommendation.")


if auto_refresh:
    st.fragment(run_every=f"{REFRESH_SECONDS}s")(_render_content)()
else:
    _render_content()
