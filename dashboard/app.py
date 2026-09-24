"""Streamlit dashboard for the AIOps platform.

Reads only from the API service: fleet health, incidents by severity/app/cluster,
detector activity, recent incidents, incident timeline and incident details, plus
on-demand AI analysis.
"""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import streamlit as st

import api_client as api

REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "15"))
SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"]

st.set_page_config(page_title="AIOps Platform", page_icon=None, layout="wide")
st.title("AIOps Platform")
st.caption("Kubernetes-native telemetry: collectors -> Kafka -> consumers -> detectors -> API.")

with st.sidebar:
    st.header("Controls")
    auto_refresh = st.toggle("Auto refresh", value=True, help=f"Refreshes every {REFRESH_SECONDS}s")
    severity_filter = st.multiselect("Severity", SEVERITY_ORDER, default=SEVERITY_ORDER)
    status_filter = st.multiselect("Incident status", ["NEW", "ONGOING", "RESOLVED"], default=["NEW", "ONGOING"])
    if st.button("Refresh now", width="stretch"):
        st.rerun()
    st.divider()
    st.caption(f"API: `{api.API_URL}`")


def _severity_chart(items: list[dict]) -> pd.DataFrame:
    counts = {level: 0 for level in SEVERITY_ORDER}
    for item in items:
        counts[item.get("severity", "info")] = counts.get(item.get("severity", "info"), 0) + 1
    return pd.DataFrame({"severity": SEVERITY_ORDER, "count": [counts[level] for level in SEVERITY_ORDER]})


def _count_chart(items: list[dict], field: str, label: str) -> pd.DataFrame:
    counts: dict[str, int] = {}
    for item in items:
        key = item.get(field) or "unassigned"
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        return pd.DataFrame({label: [], "count": []})
    frame = pd.DataFrame(sorted(counts.items(), key=lambda pair: pair[1], reverse=True), columns=[label, "count"])
    return frame.head(15)


def _timeline(items: list[dict]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame({"time": [], "incidents": []})
    frame = pd.DataFrame({"timestamp": pd.to_datetime([item["last_detected"] for item in items], utc=True)})
    frame = frame.set_index("timestamp").resample("1h").size().reset_index(name="incidents")
    return frame.rename(columns={"timestamp": "time"})


def _health_badge(payload: dict, key: str = "reachable") -> str:
    if not payload:
        return "unknown"
    return "healthy" if payload.get(key) else "degraded"


def _render_overview() -> None:
    try:
        summary = api.summary()
        anomalies = api.current_anomalies()
        kafka = api.kafka_health()
        collectors = api.collectors_health()
        detectors = api.detectors_status()
    except api.ApiError as exc:
        st.error(f"API unavailable: {exc}")
        st.info("Start the stack with `docker compose up --build`.")
        return

    kpis = st.columns(6)
    kpis[0].metric("Applications monitored", summary.get("applications_monitored", 0))
    kpis[1].metric("Healthy applications", summary.get("applications_healthy", 0))
    kpis[2].metric("Applications with anomalies", summary.get("applications_with_anomalies", 0))
    kpis[3].metric("Active incidents", summary.get("incidents_active", 0))
    kpis[4].metric("Resolved incidents", summary.get("incidents_resolved", 0))
    by_severity = summary.get("incidents_by_severity", {})
    kpis[5].metric("Critical / High", f"{by_severity.get('critical', 0)} / {by_severity.get('high', 0)}")

    platform = st.columns(4)
    platform[0].metric("Collector", _health_badge(collectors.get("collector", {})))
    platform[1].metric("Kafka consumers", _health_badge(kafka.get("consumer", {})))
    platform[2].metric("Detector service", _health_badge(detectors.get("detector_service", {})))
    platform[3].metric("Inference", "configured" if summary.get("ai_configured", True) else "n/a")

    chart_cols = st.columns(3)
    with chart_cols[0]:
        st.subheader("Incidents by severity")
        st.bar_chart(_severity_chart(anomalies), x="severity", y="count")
    with chart_cols[1]:
        st.subheader("Incidents by application")
        st.bar_chart(_count_chart(anomalies, "app_id", "application"), x="application", y="count")
    with chart_cols[2]:
        st.subheader("Incidents by cluster")
        st.bar_chart(_count_chart(anomalies, "cluster_id", "cluster"), x="cluster", y="count")

    st.subheader("Incident timeline")
    st.line_chart(_timeline(anomalies), x="time", y="incidents")

    activity = detectors.get("detectors", [])
    if activity:
        st.subheader("Detector activity")
        st.dataframe(pd.DataFrame(activity), hide_index=True, width="stretch")


def _render_incidents() -> None:
    try:
        items = api.incidents(status=",".join(status_filter) if status_filter else None, severity=",".join(severity_filter) if severity_filter else None)
    except api.ApiError as exc:
        st.error(f"Could not load incidents: {exc}")
        return

    st.subheader("Recent incidents")
    if not items:
        st.info("No incidents match the current filters.")
        return

    table = pd.DataFrame(
        [
            {
                "incident_id": item["incident_id"],
                "severity": item["severity"],
                "status": item["status"],
                "application": item["app_id"],
                "cluster": item.get("cluster_id"),
                "anomaly": item["anomaly_type"],
                "observations": item.get("observation_count"),
                "last_detected": item.get("last_detected"),
                #"ai": (item.get("ai") or {}).get("status"),
            }
            for item in items
        ]
    )
    event = st.dataframe(
        table,
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        key="recent-incidents",
    )
    selected_rows = event.selection.rows if event and event.selection else []
    if not selected_rows:
        st.caption("Select a row to see its evidence and AI analysis.")
        return

    _render_detail(items[selected_rows[0]])


def _render_detail(item: dict) -> None:
    st.divider()
    header = st.columns([3, 1])
    header[0].subheader(f"{item['app_id']} — {item['anomaly_type']}")
    header[1].metric("Severity", str(item["severity"]).upper())

    meta = st.columns(5)
    meta[0].metric("Status", item["status"])
    meta[1].metric("Cluster", item.get("cluster_id") or "n/a")
    meta[2].metric("Detector", item.get("detector_id") or "n/a")
    meta[3].metric("Kafka partition", item.get("kafka_partition"))
    meta[4].metric("Observations", item.get("observation_count"))

    st.caption(
        f"fingerprint={item.get('fingerprint')}  first_detected={item.get('first_detected')}  "
        f"last_detected={item.get('last_detected')}"
    )

    left, right = st.columns(2)
    with left:
        st.markdown("**Evidence**")
        st.json(item.get("evidence", []))
    with right:
        st.markdown("**AI Analysis**")
        analysis = item.get("ai")
        if not analysis:
            st.info("No AI Analysis Yet.")
        else:
            status = analysis.get("status")
            if status == "failed":
                st.warning(f"AI analysis failed: {analysis.get('error')}")
            if analysis.get("explanation"):
                st.markdown(f"**Explanation.** {analysis['explanation']}")
            if analysis.get("root_causes"):
                st.markdown("**Root-cause Hypotheses**")
                for cause in analysis["root_causes"]:
                    st.markdown(f"- {cause}")
            if analysis.get("remediation"):
                st.markdown("**Recommended Remediation**")
                for index, step in enumerate(analysis["remediation"], start=1):
                    st.markdown(f"{index}. {step}")
            if analysis.get("next_action"):
                st.markdown(f"**Next Diagnostic Action.** {analysis['next_action']}")
            if analysis.get("confidence") is not None:
                st.progress(float(analysis["confidence"]), text=f"Confidence: {float(analysis['confidence']):.0%}")
            st.caption(f"model={analysis.get('model')} latency={analysis.get('latency_seconds')}s")

        if st.button(f"Analyze {item['incident_id']} with AI", key=f"analyze-{item['incident_id']}"):
            with st.spinner("Calling the inference service..."):
                try:
                    result = api.analyze(item["incident_id"])
                    st.success("Analysis complete.")
                    st.json(result.get("ai"))
                except api.ApiError as exc:
                    st.error(str(exc))


def _render_content() -> None:
    _render_overview()
    st.divider()
    _render_incidents()


if auto_refresh:
    st.fragment(run_every=f"{REFRESH_SECONDS}s")(_render_content)()
else:
    _render_content()
