import json
import os
from urllib import error, request

import streamlit as st

st.set_page_config(page_title="Snowflake Cost Copilot", layout="wide")
st.title("Snowflake Cost Copilot")

api_url = os.environ.get("CHAT_API_URL", "http://127.0.0.1:8000/chat")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

st.sidebar.header("Controls")
time_range = st.sidebar.selectbox("Time range", options=["24h", "7d", "30d"], index=1)
quick = st.sidebar.radio(
    "Quick prompts",
    options=[
        "Top Jobs",
        "Warehouses",
        "Pipes",
        "Recommendations",
    ],
)

quick_prompt_map = {
    "Top Jobs": "Top 5 expensive query tags last 7 days",
    "Warehouses": "Which warehouses have idle burn?",
    "Pipes": "Show ingestion failures and small-file patterns",
    "Recommendations": "Show top recommendations this week",
}

col1, col2 = st.columns([5, 1])
with col1:
    user_q = st.text_input("Ask the copilot", value="")
with col2:
    ask = st.button("Send")

if st.button("Use quick prompt"):
    user_q = quick_prompt_map[quick]
    ask = True


def call_api(question: str, tr: str):
    payload = {"question": question, "time_range": tr}
    req = request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.URLError as exc:
        return {"summary": f"API request failed: {exc}", "top_offenders": [], "recommended_actions": [], "sql_snippets": []}


if ask and user_q.strip():
    response = call_api(user_q.strip(), time_range)
    st.session_state.chat_history.append({"question": user_q.strip(), "response": response})

st.subheader("Chat")
for item in reversed(st.session_state.chat_history):
    st.markdown(f"**You:** {item['question']}")
    r = item["response"]
    st.markdown(f"**Copilot:** {r.get('summary', '')}")
    st.caption(r.get("grounding_note", ""))

    offenders = r.get("top_offenders", [])
    if offenders:
        st.markdown("**Top offenders / metrics**")
        st.dataframe(offenders, use_container_width=True)

    evidence_refs = r.get("evidence_references", [])
    if evidence_refs:
        st.markdown("**Evidence references**")
        st.code(", ".join(evidence_refs), language="text")

    actions = r.get("recommended_actions", [])
    if actions:
        st.markdown("**Recommended actions**")
        st.dataframe(actions, use_container_width=True)

    snippets = r.get("sql_snippets", [])
    if snippets:
        st.markdown("**SQL you can run in Snowflake**")
        for i, snippet in enumerate(snippets, start=1):
            st.code(snippet, language="sql")
