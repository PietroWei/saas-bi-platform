"""Streamlit entrypoint — landing page.

Streamlit auto-discovers files under ``pages/`` and exposes them as
additional pages in the sidebar, so this script only needs to render
the home/landing content.
"""

from __future__ import annotations

import streamlit as st

from api_client import BACKEND_URL, list_companies

st.set_page_config(
    page_title="SaaS BI Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main() -> None:
    st.title("📊 SaaS BI Platform")
    st.markdown(
        "Public-signal analytics for SaaS companies — reviews, funding, and "
        "engineering activity blended into a single **health score**."
    )

    companies = list_companies(limit=5)
    if companies:
        st.subheader("Top-scoring companies right now")
        st.dataframe(companies, use_container_width=True, hide_index=True)
    else:
        st.info(
            "No data yet. In Airflow, trigger the three ingestion DAGs, "
            "then run `docker compose run --rm dbt-runner dbt build`."
        )

    st.divider()
    st.markdown(
        "**Where to next?**\n\n"
        "- **Company View** — perception of your own company.\n"
        "- **Analyst View** — due-diligence on any tracked company.\n\n"
        "Use the sidebar to switch pages."
    )

    st.sidebar.caption("Backend:")
    st.sidebar.code(BACKEND_URL, language="text")


if __name__ == "__main__":
    main()
