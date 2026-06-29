"""
Distributed Log System — PaaS Demo & Control Panel
Run: streamlit run demo.py
"""

import os
import time
from datetime import datetime, timedelta, timezone

import httpx
import pandas as pd
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Distributed Log System",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📡 DLS PaaS")
    api_base = st.text_input(
        "API Base URL", value=os.environ.get("API_BASE_URL", "http://localhost:8000")
    )
    api_key = st.text_input(
        "API Key", type="password", value=st.session_state.get("api_key", "")
    )
    if api_key:
        st.session_state["api_key"] = api_key

    # Connection status
    def check_connection():
        try:
            r = httpx.get(f"{api_base}/health", timeout=3)
            return r.status_code == 200
        except Exception:
            return False

    connected = check_connection()
    if connected:
        st.success("● Connected")
    else:
        st.error("● Disconnected")

    auto_refresh = st.toggle("Auto-refresh", value=True)
    refresh_interval = st.slider("Refresh interval (s)", 2, 30, 5)

    st.divider()
    st.caption("Distributed Log System v1.0.0")


# ── Helpers ───────────────────────────────────────────────────────────────────
HEADERS = {"X-API-Key": st.session_state.get("api_key", "")}


def api_get(path: str, params: dict | None = None):
    try:
        r = httpx.get(f"{api_base}{path}", headers=HEADERS, params=params, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        st.error(f"API error: {exc}")
        return None


def api_post(path: str, json: dict | None = None, headers_extra: dict | None = None):
    h = {**HEADERS, **(headers_extra or {})}
    try:
        r = httpx.post(f"{api_base}{path}", headers=h, json=json, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        st.error(f"API error: {exc}")
        return None


def api_delete(path: str):
    try:
        r = httpx.delete(f"{api_base}{path}", headers=HEADERS, timeout=8)
        return r.status_code in (200, 204)
    except Exception:
        return False


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "🖥 System Overview",
        "📋 Live Log Stream",
        "💓 Node Health",
        "📊 Analytics",
        "⚙ Configuration",
    ]
)


# ════════════════════════════════════════════════════════════════════════════
# Tab 1 — System Overview
# ════════════════════════════════════════════════════════════════════════════
with tab1:
    st.header("System Overview")

    if auto_refresh:
        st.cache_data.clear()

    nodes_data = api_get("/v1/nodes")
    stats_data = api_get(
        "/v1/logs/stats",
        {"from_ts": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()},
    )

    if nodes_data:
        nodes = nodes_data.get("nodes", [])
        active = sum(1 for n in nodes if n.get("status") == "active")
        failed = sum(1 for n in nodes if n.get("status") == "failed")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Nodes", len(nodes))
        c2.metric("Active", active, delta=None)
        c3.metric(
            "Failed",
            failed,
            delta=f"-{failed}" if failed else None,
            delta_color="inverse",
        )

        if stats_data:
            err_rate = stats_data.get("error_rate_pct", 0)
            c4.metric("Error Rate (5m)", f"{err_rate:.1f}%", delta_color="inverse")

        st.subheader("Node Status")
        cols = st.columns(min(len(nodes), 4) or 1)
        for i, node in enumerate(nodes):
            col = cols[i % len(cols)]
            status = node.get("status", "unknown")
            color = "🟢" if status == "active" else "🔴" if status == "failed" else "⚪"
            col.markdown(
                f"{color} **{node.get('service_name', node['node_id'][:8])}**  \n"
                f"`{node['node_id'][:12]}...`  \n"
                f"Status: `{status}`"
            )

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════
# Tab 2 — Live Log Stream
# ════════════════════════════════════════════════════════════════════════════
with tab2:
    st.header("Live Log Stream")

    col_a, col_b, col_c = st.columns([2, 1, 1])
    with col_a:
        service_filter = st.selectbox(
            "Service",
            [
                "All",
                "Inventory_Service",
                "Order_Service",
                "Payment_Service",
                "Shipping_Service",
            ],
        )
    with col_b:
        level_filter = st.selectbox("Log Level", ["All", "INFO", "WARN", "ERROR"])
    with col_c:
        page_size = st.number_input("Rows", min_value=10, max_value=500, value=50)

    paused = st.toggle("Pause stream", value=False)

    params: dict = {"page_size": page_size}
    if service_filter != "All":
        params["service_name"] = service_filter
    if level_filter != "All":
        params["log_level"] = level_filter

    if not paused:
        logs_resp = api_get("/v1/logs", params)
        if logs_resp:
            logs = logs_resp.get("logs", [])
            st.caption(f"Showing {len(logs)} of {logs_resp.get('total', 0)} logs")
            if logs:
                df = pd.DataFrame(logs)
                df = df[
                    ["timestamp", "log_level", "service_name", "message", "node_id"]
                ].rename(
                    columns={
                        "timestamp": "Timestamp",
                        "log_level": "Level",
                        "service_name": "Service",
                        "message": "Message",
                        "node_id": "Node",
                    }
                )

                def _color(row):
                    if row["Level"] == "ERROR":
                        return ["background-color: #ffcccc"] * len(row)
                    if row["Level"] == "WARN":
                        return ["background-color: #fff3cc"] * len(row)
                    return [""] * len(row)

                st.dataframe(
                    df.style.apply(_color, axis=1), use_container_width=True, height=500
                )

                # Export
                csv = df.to_csv(index=False)
                st.download_button("Download CSV", csv, "logs.csv", "text/csv")

    if auto_refresh and not paused:
        time.sleep(refresh_interval)
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════
# Tab 3 — Node Health
# ════════════════════════════════════════════════════════════════════════════
with tab3:
    st.header("Node Health")

    nodes_data = api_get("/v1/nodes")
    if nodes_data:
        nodes = nodes_data.get("nodes", [])
        if not nodes:
            st.info("No nodes registered yet.")
        else:
            node_options = {
                f"{n.get('service_name', n['node_id'])} — {n['node_id'][:12]}": n[
                    "node_id"
                ]
                for n in nodes
            }
            selected_label = st.selectbox("Select Node", list(node_options.keys()))
            selected_id = node_options[selected_label]

            node = next((n for n in nodes if n["node_id"] == selected_id), None)
            if node:
                c1, c2, c3 = st.columns(3)
                status = node.get("status", "unknown")
                c1.metric("Status", status.upper())
                c2.metric("Service", node.get("service_name", "-"))
                c3.metric("Last Updated", node.get("updated_at", "-")[:19])

            hb_resp = api_get(f"/v1/nodes/{selected_id}/heartbeats", {"limit": 50})
            if hb_resp:
                hbs = hb_resp.get("heartbeats", [])
                if hbs:
                    df_hb = pd.DataFrame(hbs)[["timestamp", "status"]]
                    df_hb["timestamp"] = pd.to_datetime(df_hb["timestamp"])
                    df_hb["up"] = df_hb["status"].apply(lambda s: 1 if s == "UP" else 0)
                    st.line_chart(df_hb.set_index("timestamp")["up"], height=200)

            st.divider()
            if st.button("🔄 Trigger Restart", type="primary"):
                result = api_post(f"/v1/nodes/{selected_id}/restart")
                if result:
                    st.success(
                        f"Restart triggered via {result.get('backend', 'unknown')} backend"
                    )
                else:
                    st.error("Restart failed")


# ════════════════════════════════════════════════════════════════════════════
# Tab 4 — Analytics
# ════════════════════════════════════════════════════════════════════════════
with tab4:
    st.header("Analytics")

    col_d, col_e = st.columns(2)
    with col_d:
        from_dt = st.date_input(
            "From", datetime.now(timezone.utc).date() - timedelta(days=1)
        )
    with col_e:
        to_dt = st.date_input("To", datetime.now(timezone.utc).date())

    from_ts = datetime.combine(from_dt, datetime.min.time()).isoformat() + "Z"
    to_ts = datetime.combine(to_dt, datetime.max.time()).isoformat() + "Z"

    stats = api_get("/v1/logs/stats", {"from_ts": from_ts, "to_ts": to_ts})
    if stats:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Logs", stats["total_logs"])
        c2.metric("Error Rate", f"{stats['error_rate_pct']:.1f}%")
        c3.metric("Services", len(stats["by_service"]))

        col_f, col_g = st.columns(2)
        with col_f:
            st.subheader("Logs by Level")
            df_level = pd.DataFrame(
                stats["by_level"].items(), columns=["Level", "Count"]
            )
            st.bar_chart(df_level.set_index("Level"))

        with col_g:
            st.subheader("Logs by Service")
            df_svc = pd.DataFrame(
                stats["by_service"].items(), columns=["Service", "Count"]
            )
            st.bar_chart(df_svc.set_index("Service"))


# ════════════════════════════════════════════════════════════════════════════
# Tab 5 — Configuration
# ════════════════════════════════════════════════════════════════════════════
with tab5:
    st.header("Configuration")

    st.subheader("Webhook Management")
    wh_resp = api_get("/v1/webhooks")
    if wh_resp:
        webhooks_list = wh_resp.get("webhooks", [])
        if webhooks_list:
            for wh in webhooks_list:
                col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
                col1.code(wh["url"])
                col2.write(", ".join(wh["events"]))
                if col3.button("Test", key=f"test_{wh['webhook_id']}"):
                    res = api_post(f"/v1/webhooks/{wh['webhook_id']}/test")
                    if res:
                        st.info(f"Test sent — HTTP {res.get('status_code', 'err')}")
                if col4.button("Delete", key=f"del_{wh['webhook_id']}"):
                    if api_delete(f"/v1/webhooks/{wh['webhook_id']}"):
                        st.success("Deleted")
                        st.rerun()
        else:
            st.info("No webhooks registered.")

    st.subheader("Add Webhook")
    with st.form("add_webhook"):
        wh_url = st.text_input(
            "Webhook URL", placeholder="https://your-server.com/webhook"
        )
        wh_events = st.multiselect(
            "Events", ["node_down", "node_up", "error_spike"], default=["node_down"]
        )
        wh_secret = st.text_input("HMAC Secret (optional)", type="password")
        submitted = st.form_submit_button("Register Webhook")
        if submitted and wh_url:
            result = api_post(
                "/v1/webhooks",
                {"url": wh_url, "events": wh_events, "secret": wh_secret},
            )
            if result:
                st.success(f"Webhook registered: {result.get('webhook_id')}")
                st.rerun()

    st.divider()
    st.subheader("API Quick Reference")
    st.code(
        """# Get your API key from your admin
curl -X POST https://your-host/admin/keys \\
  -H "X-Admin-Secret: <admin_secret>" \\
  -H "Content-Type: application/json" \\
  -d '{"owner_email": "you@example.com"}'

# Query logs
curl https://your-host/v1/logs?log_level=ERROR&page_size=20 \\
  -H "X-API-Key: dls_<your_key>"

# Get node status
curl https://your-host/v1/nodes \\
  -H "X-API-Key: dls_<your_key>"

# Full API docs: https://your-host/docs
""",
        language="bash",
    )
